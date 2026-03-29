"""
ZY-Invest PDF Statement Generator  v3.0.0
Professional layout:
- Logo in header, footer on every page
- Letter-style layout for personal statements
- Disclaimer always before last-page footer
- NTA chart with smart axis scaling
- All 5 statement types: factsheet, subscription, redemption, dividend, account
"""
import io, os
from datetime import date, datetime
from decimal import Decimal

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, Image, KeepTogether, PageBreak, Frame, PageTemplate,
    BaseDocTemplate
)
from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT

# ── Brand ──────────────────────────────────────────────────────
BLUE       = colors.HexColor('#1565C0')
BLUE_LIGHT = colors.HexColor('#EBF4FF')
GREEN      = colors.HexColor('#2E7D32')
RED        = colors.HexColor('#C62828')
GRAY1      = colors.HexColor('#1F2937')
GRAY2      = colors.HexColor('#4B5563')
GRAY3      = colors.HexColor('#9CA3AF')
GRAY4      = colors.HexColor('#F3F4F6')
GRAY5      = colors.HexColor('#E5E7EB')
WHITE      = colors.white

W, H       = A4
ML = MR    = 15*mm
MT = MB    = 12*mm
CW         = W - ML - MR   # content width = 168mm

LOGO_PATH  = os.path.join(os.path.dirname(__file__), '../../assets/img/logo_zy.png')

# ── Formatters ─────────────────────────────────────────────────
def fm(v, dp=2):
    if v is None: return '—'
    try: return f"RM {float(v):,.{dp}f}"
    except: return '—'

def fn(v, dp=4):
    if v is None: return '—'
    try: return f"{float(v):,.{dp}f}"
    except: return '—'

def fd(v):
    if not v: return '—'
    if isinstance(v, (date, datetime)): return v.strftime('%d %b %Y')
    try: return datetime.fromisoformat(str(v)).strftime('%d %b %Y')
    except: return str(v)

def fp(v):
    if v is None: return '—'
    try:
        f = float(v)
        return f"+{f:.2f}%" if f >= 0 else f"{f:.2f}%"
    except: return '—'

# ── Styles ─────────────────────────────────────────────────────
def S():
    def ps(n, **kw): return ParagraphStyle(n, **kw)
    return {
        'h1':    ps('h1',  fontName='Helvetica-Bold', fontSize=14, textColor=GRAY1),
        'h2':    ps('h2',  fontName='Helvetica-Bold', fontSize=11, textColor=GRAY1, spaceAfter=2),
        'h3':    ps('h3',  fontName='Helvetica-Bold', fontSize=9,  textColor=BLUE, spaceBefore=6, spaceAfter=2),
        'body':  ps('body',fontName='Helvetica', fontSize=8.5, textColor=GRAY1, leading=13),
        'small': ps('sm',  fontName='Helvetica', fontSize=7.5, textColor=GRAY2, leading=11),
        'tiny':  ps('tiny',fontName='Helvetica', fontSize=7,   textColor=GRAY3, leading=10),
        'bold':  ps('b',   fontName='Helvetica-Bold', fontSize=8.5, textColor=GRAY1),
        'right': ps('r',   fontName='Helvetica', fontSize=8.5, textColor=GRAY1, alignment=TA_RIGHT),
        'right_b': ps('rb',fontName='Helvetica-Bold', fontSize=8.5, textColor=GRAY1, alignment=TA_RIGHT),
        'center':ps('c',   fontName='Helvetica', fontSize=8.5, textColor=GRAY1, alignment=TA_CENTER),
        'notice':ps('n',   fontName='Helvetica', fontSize=7.5, textColor=GRAY2, leading=11),
    }

# ── Charts ──────────────────────────────────────────────────────
CC = ['#1565C0','#2E7D32','#E65100','#7B1FA2','#00838F','#BF360C','#4527A0','#558B2F']

def nta_line_chart(dates, ntas, width=CW, height=55*mm):
    """NTA history line chart with smart axis scaling and data labels."""
    if not ntas: return None
    lo, hi = min(ntas), max(ntas)
    pad    = (hi - lo) * 0.15 if hi > lo else 0.05
    y_min  = max(0, lo - pad)
    y_max  = hi + pad
    # Round to nearest 0.05
    y_min  = round(y_min / 0.05) * 0.05
    y_max  = round(y_max / 0.05 + 1) * 0.05

    fig, ax = plt.subplots(figsize=(width/mm/3.78, height/mm/3.78), dpi=150)
    x = list(range(len(dates)))
    ax.plot(x, ntas, color='#1565C0', linewidth=1.5, zorder=3)
    ax.fill_between(x, ntas, y_min, alpha=0.07, color='#1565C0')

    # X ticks — max 10
    step = max(1, len(dates)//10)
    ticks = list(range(0, len(dates), step))
    ax.set_xticks(ticks)
    ax.set_xticklabels([dates[i] for i in ticks], fontsize=5.5, rotation=30, ha='right')

    ax.set_ylim(y_min, y_max)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter('%.3f'))
    ax.tick_params(axis='y', labelsize=5.5)
    ax.grid(axis='y', alpha=0.3, linewidth=0.4, linestyle='--')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#E5E7EB')
    ax.spines['bottom'].set_color('#E5E7EB')

    # Annotate first and last
    for idx, label in [(0, f'{ntas[0]:.4f}'), (len(ntas)-1, f'{ntas[-1]:.4f}')]:
        ax.annotate(label, xy=(idx, ntas[idx]),
            xytext=(5 if idx == len(ntas)-1 else -5, 4),
            textcoords='offset points',
            fontsize=5.5, color='#1565C0', fontweight='bold',
            ha='left' if idx == len(ntas)-1 else 'right')

    plt.tight_layout(pad=0.3)
    buf = io.BytesIO()
    fig.savefig(buf, format='png', transparent=True, bbox_inches='tight', dpi=150)
    plt.close(fig)
    buf.seek(0)
    return Image(buf, width=width, height=height)


def donut_chart(labels, values, size=(58*mm, 58*mm)):
    """Donut chart with % data labels on wedges."""
    if not values or sum(values) == 0: return None
    fig, ax = plt.subplots(figsize=(2.2, 2.2), dpi=150)
    wedges, texts, autotexts = ax.pie(
        values,
        labels=None,
        autopct=lambda p: f'{p:.1f}%' if p > 3 else '',
        pctdistance=0.75,
        colors=[CC[i % len(CC)] for i in range(len(values))],
        startangle=90,
        wedgeprops=dict(width=0.52, edgecolor='white', linewidth=1.2)
    )
    for at in autotexts:
        at.set_fontsize(5.5)
        at.set_color('white')
        at.set_fontweight('bold')
    ax.set_aspect('equal')
    plt.tight_layout(pad=0.1)
    buf = io.BytesIO()
    fig.savefig(buf, format='png', transparent=True, bbox_inches='tight', dpi=150)
    plt.close(fig)
    buf.seek(0)
    return Image(buf, width=size[0], height=size[1])


# ── Logo loader ─────────────────────────────────────────────────
def get_logo(w=28*mm, h=22*mm):
    if os.path.exists(LOGO_PATH):
        try: return Image(LOGO_PATH, width=w, height=h)
        except: pass
    return Paragraph('<b>ZY-Invest</b>', ParagraphStyle('lo',
        fontName='Helvetica-Bold', fontSize=14, textColor=BLUE))


# ── Page templates with persistent header/footer ───────────────
class HeaderFooterDocTemplate(BaseDocTemplate):
    """Every page gets the same compact header/footer."""
    def __init__(self, buf, logo_path, **kw):
        super().__init__(buf, **kw)
        self.logo_path = logo_path
        frame = Frame(ML, MB + 18*mm, CW, H - MT - MB - 18*mm,
                      id='body', leftPadding=0, rightPadding=0,
                      topPadding=0, bottomPadding=0)
        tpl = PageTemplate(id='main', frames=[frame],
                           onPage=self._draw_page)
        self.addPageTemplates([tpl])

    def _draw_page(self, canvas, doc):
        canvas.saveState()
        # ── Header ──
        if os.path.exists(self.logo_path):
            try:
                canvas.drawImage(self.logo_path,
                    ML, H - MT - 16*mm, width=24*mm, height=16*mm,
                    preserveAspectRatio=True, mask='auto')
            except: pass
        # Blue line under header
        canvas.setStrokeColor(BLUE)
        canvas.setLineWidth(1.5)
        canvas.line(ML, H - MT - 17.5*mm, W - MR, H - MT - 17.5*mm)

        # ── Footer ──
        canvas.setStrokeColor(GRAY5)
        canvas.setLineWidth(0.5)
        canvas.line(ML, MB + 14*mm, W - MR, MB + 14*mm)

        canvas.setFont('Helvetica', 6.5)
        canvas.setFillColor(GRAY3)
        canvas.drawString(ML, MB + 9*mm,
            'Head Office: None  |  Line: (60)11-1121 8085  |  Email: nzy.invest@gmail.com  |  Website: zy-invest.com')
        canvas.drawRightString(W - MR, MB + 9*mm,
            f'Page {doc.page}')
        canvas.restoreState()


def make_doc(buf):
    return HeaderFooterDocTemplate(buf, LOGO_PATH,
        pagesize=A4,
        leftMargin=ML, rightMargin=MR,
        topMargin=MT + 19*mm,   # extra for header
        bottomMargin=MB + 16*mm)


# ── Shared flowable builders ────────────────────────────────────
def sec_title(s, text):
    """Blue section heading with underline."""
    return [
        Paragraph(text, s['h3']),
        HRFlowable(width='100%', thickness=0.6, color=BLUE, spaceAfter=3),
    ]

def inv_info_grid(s, fields_4col):
    """4-column investor info grid (label, value, label, value)."""
    rows = []
    for row in fields_4col:
        rows.append([
            Paragraph(row[0], s['small']),
            Paragraph(str(row[1]) if row[1] else '—', s['bold']),
            Paragraph(row[2], s['small']),
            Paragraph(str(row[3]) if row[3] else '—', s['bold']),
        ])
    t = Table(rows, colWidths=[32*mm, 54*mm, 32*mm, 50*mm])
    t.setStyle(TableStyle([
        ('GRID',    (0,0),(-1,-1), 0.4, GRAY5),
        ('ROWBACKGROUNDS',(0,0),(-1,-1),[WHITE, GRAY4]),
        ('TOPPADDING',(0,0),(-1,-1), 5),
        ('BOTTOMPADDING',(0,0),(-1,-1), 5),
        ('LEFTPADDING',(0,0),(-1,-1), 6),
        ('FONTSIZE',(0,0),(-1,-1), 8),
        ('TEXTCOLOR',(0,0),(0,-1), GRAY2),
        ('TEXTCOLOR',(2,0),(2,-1), GRAY2),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
    ]))
    return t

def data_tbl(s, headers, rows, col_w, total_row=None):
    def _cell(v, bold=False, align=TA_LEFT, color=GRAY1):
        if isinstance(v, Paragraph): return v
        return Paragraph(str(v) if v is not None else '—',
            ParagraphStyle('_c', fontName='Helvetica-Bold' if bold else 'Helvetica',
                fontSize=8, textColor=color, alignment=align, leading=11))
    hrow = [Paragraph(h, ParagraphStyle('_h', fontName='Helvetica-Bold',
        fontSize=8, textColor=WHITE, alignment=TA_CENTER)) for h in headers]
    td = [hrow] + [[_cell(c) for c in row] for row in rows]
    if total_row:
        td.append([_cell(c, bold=True) for c in total_row])
    t = Table(td, colWidths=col_w, repeatRows=1)
    sty = [
        ('BACKGROUND',(0,0),(-1,0), BLUE),
        ('ROWBACKGROUNDS',(0,1),(-1, -2 if total_row else -1),[WHITE,GRAY4]),
        ('GRID',(0,0),(-1,-1), 0.4, GRAY5),
        ('TOPPADDING',(0,0),(-1,-1), 4),
        ('BOTTOMPADDING',(0,0),(-1,-1), 4),
        ('LEFTPADDING',(0,0),(-1,-1), 5),
        ('RIGHTPADDING',(0,0),(-1,-1), 5),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
    ]
    if total_row:
        sty += [
            ('BACKGROUND',(0,-1),(-1,-1), BLUE_LIGHT),
            ('LINEABOVE',(0,-1),(-1,-1), 1, BLUE),
        ]
    t.setStyle(TableStyle(sty))
    return t

def disclaimer_block(s):
    """Disclaimer text — always placed before the final footer."""
    return [
        Spacer(1, 4*mm),
        HRFlowable(width='100%', thickness=0.5, color=GRAY5),
        Spacer(1, 2*mm),
        Paragraph('<b>IMPORTANT NOTICES</b>',
            ParagraphStyle('_nt', fontName='Helvetica-Bold', fontSize=8, textColor=GRAY1)),
        Paragraph(
            '<b>1. Confidentiality:</b> This statement contains personal data and is intended solely '
            'for the recipient. Please do not share this document with any third parties.',
            s['notice']),
        Paragraph(
            '<b>2. Discrepancies:</b> Please review all figures carefully. Any discrepancies must be '
            'reported to us immediately; failure to do so may result in the recipient bearing any losses.',
            s['notice']),
        Paragraph(
            '<b>3. Digital Statements:</b> Effective 1st January 2026, all future portfolio statements '
            'will be provided exclusively via WhatsApp.',
            s['notice']),
    ]

def letter_header(s, investor_name, address_lines, title,
                  issued_date, stmt_type='', stmt_period='', page_info='1 of 1'):
    """Letter-style header block for personal statements."""
    # Right side meta
    meta = [
        ['Page No.',       f': {page_info}'],
        ['Issued Date',    f': {issued_date}'],
    ]
    if stmt_type:   meta.append(['Statement Type',   f': {stmt_type}'])
    if stmt_period: meta.append(['Statement Period', f': {stmt_period}'])
    meta += [
        ['Email Address', ': nzy.invest@gmail.com'],
        ['Telephone No.', ': (+60)11 - 1121 8085'],
    ]
    meta_t = Table(
        [[Paragraph(r[0], s['small']), Paragraph(r[1], s['small'])] for r in meta],
        colWidths=[34*mm, 78*mm])
    meta_t.setStyle(TableStyle([
        ('TOPPADDING',(0,0),(-1,-1), 1.5),
        ('BOTTOMPADDING',(0,0),(-1,-1), 1.5),
        ('LEFTPADDING',(0,0),(-1,-1), 0),
    ]))

    addr_parts = [
        Paragraph(f'<b>{investor_name.upper()}</b>',
            ParagraphStyle('_an', fontName='Helvetica-Bold', fontSize=9, textColor=GRAY1)),
    ]
    for ln in (address_lines or '').split('\n'):
        if ln.strip():
            addr_parts.append(Paragraph(ln.strip(), s['small']))

    addr_col = Table([[p] for p in addr_parts], colWidths=[72*mm])
    addr_col.setStyle(TableStyle([
        ('TOPPADDING',(0,0),(-1,-1), 1),
        ('BOTTOMPADDING',(0,0),(-1,-1), 1),
        ('LEFTPADDING',(0,0),(-1,-1), 0),
    ]))

    row = Table([[addr_col, meta_t]], colWidths=[72*mm, 114*mm])
    row.setStyle(TableStyle([
        ('VALIGN',(0,0),(-1,-1),'TOP'),
        ('LEFTPADDING',(0,0),(-1,-1), 0),
        ('RIGHTPADDING',(0,0),(-1,-1), 0),
        ('TOPPADDING',(0,0),(-1,-1), 0),
    ]))

    title_p = Paragraph(f'<b>{title}</b>',
        ParagraphStyle('_tt', fontName='Helvetica-Bold', fontSize=13,
            textColor=GRAY1, alignment=TA_RIGHT))

    return [title_p, Spacer(1, 5*mm), row, Spacer(1, 5*mm)]


# ══════════════════════════════════════════════════════════════
# 1. FACTSHEET
# ══════════════════════════════════════════════════════════════
def generate_factsheet(fund_data, holdings, performance, distributions,
                       nta_history, sector_data, manager_comment=''):
    buf = io.BytesIO()
    doc = make_doc(buf)
    s   = S()
    story = []
    today  = date.today()
    period = today.strftime('%B %Y')

    # Title block
    story.append(Paragraph(
        f'<b>ZY Family Vision Portfolio</b>',
        ParagraphStyle('ft', fontName='Helvetica-Bold', fontSize=15,
            textColor=GRAY1, alignment=TA_RIGHT)))
    story.append(Paragraph(period,
        ParagraphStyle('fp', fontName='Helvetica', fontSize=10,
            textColor=GRAY2, alignment=TA_RIGHT)))
    story.append(Spacer(1, 3*mm))
    story.append(Paragraph(
        'The portfolio aims to provide our investors with capital appreciation higher than the '
        'prevailing fixed-deposit rate by investing in a high-growth portfolio of stocks and '
        'fixed income instruments.', s['body']))
    story.append(Spacer(1, 4*mm))

    # ── Two column: Fund Details | Sector Allocation ───────────
    nta_v  = fund_data.get('current_nta', 0)
    aum_v  = fund_data.get('aum', 0)
    uni_v  = fund_data.get('total_units', 0)

    fd_rows = [
        ('Manager',               'Mr. Ng Zhi Yao'),
        ('Fund Category',         'Equity Fund'),
        ('Launch Date',           '13 December 2021'),
        ('Unit Net Asset Value',  fm(nta_v, 4)),
        ('Fund Size',             fm(aum_v)),
        ('Units in Circulation',  fn(uni_v, 2) + ' units'),
        ('Financial Year End',    '30 November'),
        ('Min. Initial Investment','RM 10,000.00'),
        ('Min. Additional',       'RM 1,000.00'),
        ('Benchmark',             'FBMKLCI, Tech Index'),
        ('Annual Mgmt Fee',       '0.10% p.a. of NAV'),
        ('Performance Fee',       '10.0% p.a. of excess return'),
        ('Distribution Policy',   'At least 80% of gross income'),
    ]
    fd_data = [[Paragraph(k, s['small']), Paragraph(v, s['bold'])] for k,v in fd_rows]
    fd_t = Table(fd_data, colWidths=[38*mm, 56*mm])
    fd_t.setStyle(TableStyle([
        ('ROWBACKGROUNDS',(0,0),(-1,-1),[WHITE,GRAY4]),
        ('GRID',(0,0),(-1,-1), 0.4, GRAY5),
        ('TOPPADDING',(0,0),(-1,-1), 3.5),
        ('BOTTOMPADDING',(0,0),(-1,-1), 3.5),
        ('LEFTPADDING',(0,0),(-1,-1), 5),
        ('FONTSIZE',(0,0),(-1,-1), 7.5),
        ('TEXTCOLOR',(0,0),(0,-1), GRAY2),
    ]))

    # Sector donut
    pie_block = [Paragraph('Sector Allocation*', s['h3']),
                 HRFlowable(width='100%', thickness=0.6, color=BLUE, spaceAfter=3)]
    if sector_data:
        lbls = [r.get('asset_class') or r.get('sector','') for r in sector_data[:6]]
        vals = [max(float(r.get('weight_pct',0)), 0.01) for r in sector_data[:6]]
        pie  = donut_chart(lbls, vals, size=(55*mm, 55*mm))
        if pie:
            legend_rows = []
            for i, r in enumerate(sector_data[:6]):
                nm  = r.get('asset_class') or r.get('sector','')
                pct = float(r.get('weight_pct',0))
                legend_rows.append([
                    Paragraph(f'<font color="{CC[i%8]}">■</font>  {nm}', s['tiny']),
                    Paragraph(f'{pct:.1f}%', s['tiny']),
                ])
            leg_t = Table(legend_rows, colWidths=[48*mm, 12*mm])
            leg_t.setStyle(TableStyle([
                ('TOPPADDING',(0,0),(-1,-1), 2),
                ('BOTTOMPADDING',(0,0),(-1,-1), 2),
                ('LEFTPADDING',(0,0),(-1,-1), 2),
            ]))
            pie_row = Table([[pie, leg_t]], colWidths=[57*mm, 62*mm])
            pie_row.setStyle(TableStyle([
                ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
                ('LEFTPADDING',(0,0),(-1,-1), 0),
            ]))
            pie_block.append(pie_row)
        else:
            pie_block.append(Paragraph('No data', s['small']))
    else:
        pie_block.append(Paragraph('No holdings data', s['small']))

    fd_title = [Paragraph('Fund Details', s['h3']),
                HRFlowable(width='100%', thickness=0.6, color=BLUE, spaceAfter=3),
                fd_t]

    # Wrap each in single-col table for proper column containment
    left_container  = Table([[fd_title]], colWidths=[96*mm])
    right_container = Table([[pie_block]], colWidths=[72*mm])
    left_container.setStyle(TableStyle([('LEFTPADDING',(0,0),(-1,-1),0),('RIGHTPADDING',(0,0),(-1,-1),4),('TOPPADDING',(0,0),(-1,-1),0),('BOTTOMPADDING',(0,0),(-1,-1),0)]))
    right_container.setStyle(TableStyle([('LEFTPADDING',(0,0),(-1,-1),0),('RIGHTPADDING',(0,0),(-1,-1),0),('TOPPADDING',(0,0),(-1,-1),0),('BOTTOMPADDING',(0,0),(-1,-1),0)]))

    two_col = Table([[left_container, right_container]], colWidths=[96*mm, 72*mm])
    two_col.setStyle(TableStyle([
        ('VALIGN',(0,0),(-1,-1),'TOP'),
        ('LEFTPADDING',(0,0),(-1,-1),0),
        ('RIGHTPADDING',(0,0),(-1,-1),0),
        ('TOPPADDING',(0,0),(-1,-1),0),
        ('BOTTOMPADDING',(0,0),(-1,-1),0),
    ]))
    story.append(two_col)
    story.append(Spacer(1, 4*mm))

    # ── Investment Strategy ────────────────────────────────────
    story += sec_title(s, 'Investment Strategy')
    story.append(Paragraph(
        'The strategic limit on asset allocation: Equities: min 60% / max 98%.  '
        'Derivatives: max 5%.  Fixed-income: min 2% / max 40%.', s['body']))
    story.append(Spacer(1, 3*mm))

    # ── NTA Chart ──────────────────────────────────────────────
    if nta_history:
        story += sec_title(s, 'Portfolio Performance Analysis')
        dates = []
        ntas  = []
        for r in nta_history:
            try:
                dt = datetime.fromisoformat(str(r.get('date','')))
                dates.append(dt.strftime('%b-%y'))
                ntas.append(float(r.get('nta', 1)))
            except: pass
        if ntas:
            chart = nta_line_chart(dates, ntas, width=CW, height=58*mm)
            if chart:
                story.append(chart)
                story.append(Spacer(1, 2*mm))

        # Performance table
        perf  = performance.get('period_returns', {})
        t_ret = fund_data.get('total_return_pct', 0)
        story.append(data_tbl(s,
            headers=['', '1M', '3M', '6M', '1Y', '3Y', 'All'],
            rows=[['Portfolio'] + [fp(perf.get(k)) for k in ['1M','3M','6M','1Y','3Y']] + [fp(t_ret)]],
            col_w=[28*mm, 23*mm, 23*mm, 23*mm, 23*mm, 23*mm, 23*mm]))
        story.append(Spacer(1, 3*mm))

    # ── Distribution History ───────────────────────────────────
    if distributions:
        story += sec_title(s, 'Distribution History')
        rows = []
        for d in distributions:
            pr = d.get('payout_ratio')
            rows.append([
                d.get('financial_year','—'),
                d.get('title','—'),
                fn(d.get('dps_sen'),2) + ' sen',
                f"{float(pr):.1f}%" if pr else '—',
            ])
        story.append(data_tbl(s,
            headers=['Year','Description','DPS (sen)','Payout Ratio'],
            rows=rows,
            col_w=[18*mm, 100*mm, 27*mm, 23*mm]))
        story.append(Spacer(1, 3*mm))

    # ── Manager's Comments ─────────────────────────────────────
    story += sec_title(s, "Manager's Comments")
    story.append(Paragraph(manager_comment or
        'Our portfolio maintains its current strategic positions. We continue to monitor market '
        'developments closely. The fund remains well-positioned relative to its investment '
        'objectives for the current financial year.', s['body']))
    story.append(Spacer(1, 3*mm))

    # ── Largest Holdings ───────────────────────────────────────
    if holdings:
        story += sec_title(s, 'Largest Holdings*')
        h_rows = [[h.get('instrument','—'),
                   f"{float(h.get('weight_pct',0)):.2f}%"] for h in holdings[:10]]
        story.append(data_tbl(s,
            headers=['Asset Name','Percentage'],
            rows=h_rows,
            col_w=[140*mm, 28*mm]))
        story.append(Paragraph(
            '* As percentage of Net Asset Value (NAV) of the fund', s['tiny']))
        story.append(Spacer(1, 2*mm))

    # Disclaimer above footer
    story.append(Spacer(1, 3*mm))
    story.append(HRFlowable(width='100%', thickness=0.5, color=GRAY5))
    story.append(Spacer(1, 2*mm))
    story.append(Paragraph('<b>Disclaimer</b>',
        ParagraphStyle('_dis', fontName='Helvetica-Bold', fontSize=8, textColor=GRAY1)))
    story.append(Paragraph(
        'Investment involves significant risk, including the potential loss of principal. '
        'Past performance is not indicative of future results. This portfolio update is intended '
        'strictly for family members only; it does not constitute a financial prospectus and is '
        'not open to the general public or external investors.', s['notice']))

    doc.build(story)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════
# 2. SUBSCRIPTION STATEMENT
# ══════════════════════════════════════════════════════════════
def generate_subscription(investor, cashflows, statement_period=''):
    buf = io.BytesIO(); doc = make_doc(buf); s = S(); story = []
    today = date.today()
    name  = investor.get('name','')
    addr  = '\n'.join(filter(None,[
        investor.get('address_line1',''), investor.get('address_line2',''),
        ' '.join(filter(None,[investor.get('postcode',''), investor.get('city','')])),
        investor.get('state',''),
    ]))
    story += letter_header(s, name, addr,
        'FUND SUBSCRIPTION STATEMENT', today.strftime('%d-%m-%Y'),
        stmt_type='Transaction Statement',
        stmt_period=statement_period or today.strftime('%d/%m/%Y'))

    story += sec_title(s, "Investor's Information")
    story.append(inv_info_grid(s, [
        ["Account Type", investor.get('account_type','Nominee Account'), "Account ID",    investor.get('account_id','—')],
        ["Registered Name", name,                                        "Settlement Type", "Banking"],
        ["Phone No.",    investor.get('phone','—'),                      "Bank Name",     investor.get('bank_name','—')],
        ["Email Address",investor.get('email','—'),                      "Bank Account No.", investor.get('bank_account_no','—')],
        ["Nominee Name", "—",                                            "Total Days Held", f"{investor.get('days_held',0)} days"],
    ]))
    story.append(Spacer(1, 4*mm))

    if cashflows:
        rows = []
        for cf in cashflows:
            amt = float(cf.get('amount',0))
            rows.append([
                fd(cf.get('date')),
                cf.get('description','Fund Subscription'),
                fm(abs(amt)) if amt else '—',
                fn(cf.get('nta_at_date'),4) if cf.get('nta_at_date') else '—',
                fn(cf.get('units'),4) if cf.get('units') else '—',
                fn(cf.get('avg_cost'),4) if cf.get('avg_cost') else '—',
            ])
        tot_inv    = sum(abs(float(cf.get('amount',0))) for cf in cashflows if float(cf.get('amount',0)) > 0)
        final_u    = sum(float(cf.get('units',0)) for cf in cashflows)
        avg_c      = cashflows[-1].get('nta_at_date') if cashflows else None
        all_rows   = [
            [fd(cashflows[0].get('date')), 'Opening','—','—','—','—'],
            *rows,
            [fd(cashflows[-1].get('date')), 'Closing', fm(tot_inv),'—', fn(final_u,4), fn(avg_c,4) if avg_c else '—'],
        ]
        story += sec_title(s, 'Principal Transaction')
        story.append(data_tbl(s,
            headers=['Date','Description','Investment Value','Subscription Price','Unit Balanced','Average Cost'],
            rows=all_rows, col_w=[24*mm,38*mm,30*mm,28*mm,26*mm,22*mm]))

    story += disclaimer_block(s)
    doc.build(story)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════
# 3. REDEMPTION STATEMENT
# ══════════════════════════════════════════════════════════════
def generate_redemption(investor, cashflows, statement_period=''):
    buf = io.BytesIO(); doc = make_doc(buf); s = S(); story = []
    today = date.today()
    name  = investor.get('name','')
    addr  = '\n'.join(filter(None,[
        investor.get('address_line1',''), investor.get('address_line2',''),
        ' '.join(filter(None,[investor.get('postcode',''), investor.get('city','')])),
        investor.get('state',''),
    ]))
    story += letter_header(s, name, addr,
        'FUND REDEMPTION STATEMENT', today.strftime('%d-%m-%Y'),
        stmt_type='Transaction Statement',
        stmt_period=statement_period or today.strftime('%d/%m/%Y'))

    story += sec_title(s, "Investor's Information")
    story.append(inv_info_grid(s, [
        ["Account Type", investor.get('account_type','Nominee Account'), "Account ID",    investor.get('account_id','—')],
        ["Registered Name", name,                                        "Settlement Type", "Banking"],
        ["Phone No.",    investor.get('phone','—'),                      "Bank Name",     investor.get('bank_name','—')],
        ["Email Address",investor.get('email','—'),                      "Bank Account No.", investor.get('bank_account_no','—')],
        ["Nominee Name", "—",                                            "Total Days Held", f"{investor.get('days_held',0)} days"],
    ]))
    story.append(Spacer(1, 4*mm))

    if cashflows:
        rows = []
        for cf in cashflows:
            amt = float(cf.get('amount',0))
            rows.append([
                fd(cf.get('date')),
                cf.get('description','Fund Redemption'),
                fm(abs(amt)) if amt else '—',
                fn(cf.get('nta_at_date'),4) if cf.get('nta_at_date') else '—',
                fn(abs(float(cf.get('units',0))),4) if cf.get('units') else '—',
                fn(cf.get('avg_cost'),4) if cf.get('avg_cost') else '—',
            ])
        tot_red  = sum(abs(float(cf.get('amount',0))) for cf in cashflows if float(cf.get('amount',0)) < 0)
        final_u  = sum(float(cf.get('units',0)) for cf in cashflows)
        all_rows = [
            [fd(cashflows[0].get('date')), 'Opening','—','—','—','—'],
            *rows,
            [fd(cashflows[-1].get('date')), 'Closing', fm(tot_red),'—', fn(final_u,4) if final_u > 0 else '0.0000','—'],
        ]
        story += sec_title(s, 'Redemption Transaction')
        story.append(data_tbl(s,
            headers=['Date','Description','Redemption Value','Redemption Price','Units Redeemed','Average Cost'],
            rows=all_rows, col_w=[24*mm,38*mm,30*mm,28*mm,26*mm,22*mm]))

    story += disclaimer_block(s)
    doc.build(story)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════
# 4. DIVIDEND PAYMENT STATEMENT
# ══════════════════════════════════════════════════════════════
def generate_dividend_statement(investor, distributions, financial_year):
    buf = io.BytesIO(); doc = make_doc(buf); s = S(); story = []
    today = date.today()
    name  = investor.get('name','')
    addr  = '\n'.join(filter(None,[
        investor.get('address_line1',''), investor.get('address_line2',''),
        ' '.join(filter(None,[investor.get('postcode',''), investor.get('city','')])),
        investor.get('state',''),
    ]))
    pmt_period = fd(distributions[0].get('pmt_date')) if distributions else today.strftime('%d/%m/%Y')
    story += letter_header(s, name, addr,
        'DIVIDEND PAYMENT STATEMENT', today.strftime('%d-%m-%Y'),
        stmt_type=financial_year, stmt_period=pmt_period)

    story += sec_title(s, "Investor's Information")
    story.append(inv_info_grid(s, [
        ["Investor's Name", name,                      "Settlement Type",   "Banking"],
        ["Phone Number",    investor.get('phone','—'), "Bank Name",         investor.get('bank_name','—')],
        ["Email Address",   investor.get('email','—'), "Bank Account No.",  investor.get('bank_account_no','—')],
    ]))
    story.append(Spacer(1, 4*mm))

    if distributions:
        tot_dps = tot_amt = 0.0
        rows = []
        for d in distributions:
            dps  = float(d.get('dps_sen',0))
            u    = float(d.get('units_at_ex_date',0))
            amt  = float(d.get('amount',0))
            eps  = d.get('eps'); dpr = d.get('payout_ratio')
            tot_dps += dps; tot_amt += amt
            rows.append([
                fd(d.get('pmt_date') or d.get('ex_date')),
                d.get('title', d.get('dist_type','')).replace('_',' ').title(),
                fn(u, 4),
                fn(eps, 2) if eps else '—',
                f"{float(dpr):.1f}%" if dpr else '—',
                fn(dps, 2),
                Paragraph(f'<b>{fm(amt)}</b>',
                    ParagraphStyle('_am', fontName='Helvetica-Bold', fontSize=8,
                        textColor=BLUE, alignment=TA_RIGHT)),
            ])
        story += sec_title(s, 'Dividend Details')
        story.append(data_tbl(s,
            headers=['Date','Description','Holding Units','EPS','DPR','DPS (sen)','Amount'],
            rows=rows,
            col_w=[24*mm,40*mm,24*mm,14*mm,14*mm,18*mm,34*mm],
            total_row=['','Total','','','', fn(tot_dps,2),
                Paragraph(f'<b>{fm(tot_amt)}</b>',
                    ParagraphStyle('_ta', fontName='Helvetica-Bold', fontSize=8,
                        textColor=BLUE, alignment=TA_RIGHT))]))
        story.append(Paragraph(
            'Notes: EPS = Earning Per Share  |  DPR = Dividend Payout Ratio  |  DPS = Dividend Per Share (sen)',
            s['tiny']))

    story += disclaimer_block(s)
    doc.build(story)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════
# 5. INVESTMENT ACCOUNT STATEMENT (Annual, 2 pages)
# ══════════════════════════════════════════════════════════════
def generate_account_statement(investor, summary, cashflows, dist_history,
                                statement_period, financial_year):
    buf = io.BytesIO(); doc = make_doc(buf); s = S(); story = []
    today = date.today()
    name  = investor.get('name','')
    addr  = '\n'.join(filter(None,[
        investor.get('address_line1',''), investor.get('address_line2',''),
        ' '.join(filter(None,[investor.get('postcode',''), investor.get('city','')])),
        investor.get('state',''),
    ]))

    # ── Page 1 ─────────────────────────────────────────────────
    story += letter_header(s, name, addr,
        'INVESTMENT ACCOUNT STATEMENT', today.strftime('%d-%m-%Y'),
        stmt_type='Annually', stmt_period=statement_period, page_info='1 of 2')

    story += sec_title(s, "Investor's Information")
    story.append(inv_info_grid(s, [
        ["Account Type",    summary.get('account_type','Nominee Account'), "Account ID",    investor.get('account_id','—')],
        ["Registered Name", name,                                          "Settlement Type","Banking"],
        ["Phone No.",       investor.get('phone','—'),                     "Bank Name",     investor.get('bank_name','—')],
        ["Email Address",   investor.get('email','—'),                     "Bank Account No.", investor.get('bank_account_no','—')],
        ["Nominee Name",    "—",                                           "Total Days Held",f"{summary.get('days_held',0)} days"],
    ]))
    story.append(Spacer(1, 4*mm))

    # Account summary
    nta_v   = float(summary.get('current_nta',0))
    units_v = float(summary.get('units',0))
    cost_v  = float(summary.get('total_costs',0))
    mv_v    = float(summary.get('market_value',0))
    unr_v   = float(summary.get('unrealized_pl',0))
    rea_v   = float(summary.get('realized_pl',0))
    div_v   = float(summary.get('dividends_received',0))
    adj_v   = float(summary.get('adjustment',0))
    tpl     = unr_v + rea_v + div_v + adj_v
    tpct    = (tpl / abs(cost_v) * 100) if cost_v else 0
    irr     = summary.get('irr')
    avg_c   = cost_v / units_v if units_v else 0

    def _color_p(v, fmt_fn):
        if v is None: return Paragraph('—', s['right'])
        color = GREEN if float(v) >= 0 else RED
        return Paragraph(f'<b>{fmt_fn(v)}</b>',
            ParagraphStyle('_cp', fontName='Helvetica-Bold', fontSize=8,
                textColor=color, alignment=TA_RIGHT))

    story += sec_title(s, 'Account Summary')
    sum_rows = [
        ['(a)', 'Latest Fund Price',  fn(units_v,4), fn(nta_v,4),    fm(mv_v)],
        ['(b)', 'Subscription Cost',  fn(units_v,4), fn(avg_c,4),    f'({fm(cost_v)})'],
        ['(c)', 'Unrealized P&L:  (a) + (b)', '', '',                fm(unr_v)],
        ['(d)', 'Realized Profit & Loss', '', '',                    fm(rea_v) if rea_v else '—'],
        ['(e)', 'Dividend Received', '', '',                         fm(div_v) if div_v else '—'],
        ['(f)', f'Adjustment: Initial Capital {fm(abs(adj_v))}','','',
         f'({fm(abs(adj_v))})' if adj_v < 0 else (fm(adj_v) if adj_v else '—')],
    ]
    def _b(t): return Paragraph(f'<b>{t}</b>',
        ParagraphStyle('_bl', fontName='Helvetica-Bold', fontSize=8, textColor=GRAY1))

    sum_hdr = ['','Fields','Holding Units','Average Price','Total Value (RM)']
    sum_data = [
        [Paragraph(h, ParagraphStyle('_sh', fontName='Helvetica-Bold', fontSize=8,
            textColor=WHITE, alignment=TA_CENTER)) for h in sum_hdr]
    ]
    for row in sum_rows:
        sum_data.append([
            Paragraph(row[0], s['small']),
            Paragraph(row[1], s['body']),
            Paragraph(row[2], ParagraphStyle('_sr',fontName='Helvetica',fontSize=8,textColor=GRAY1,alignment=TA_RIGHT)),
            Paragraph(row[3], ParagraphStyle('_sr2',fontName='Helvetica',fontSize=8,textColor=GRAY1,alignment=TA_RIGHT)),
            Paragraph(row[4], ParagraphStyle('_sr3',fontName='Helvetica',fontSize=8,textColor=GRAY1,alignment=TA_RIGHT)),
        ])
    tc, tpc = (GREEN if tpl >= 0 else RED), (GREEN if tpct >= 0 else RED)
    ic = GREEN if irr and float(irr) >= 0 else RED
    sum_data.append(['', _b('Total Profit & Loss:  (c) + (d) + (e) + (f)'), '', '',
        Paragraph(f'<b>{fm(tpl)}</b>',
            ParagraphStyle('_tp',fontName='Helvetica-Bold',fontSize=8,textColor=tc,alignment=TA_RIGHT))])
    sum_data.append(['', _b('Total Performance %'), '', '',
        Paragraph(f'<b>{fp(tpct)}</b>',
            ParagraphStyle('_tpct',fontName='Helvetica-Bold',fontSize=8,textColor=tpc,alignment=TA_RIGHT))])
    sum_data.append(['', _b('Annualised Performance* %'), '', '',
        Paragraph(f'<b>{fp(irr) if irr else "—"}</b>',
            ParagraphStyle('_irr',fontName='Helvetica-Bold',fontSize=8,textColor=ic,alignment=TA_RIGHT))])

    sum_t = Table(sum_data, colWidths=[8*mm, 82*mm, 26*mm, 26*mm, 26*mm])
    sum_t.setStyle(TableStyle([
        ('BACKGROUND',(0,0),(-1,0), BLUE),
        ('ROWBACKGROUNDS',(0,1),(-1,-4),[WHITE,GRAY4]),
        ('GRID',(0,0),(-1,-1), 0.4, GRAY5),
        ('TOPPADDING',(0,0),(-1,-1), 4),('BOTTOMPADDING',(0,0),(-1,-1), 4),
        ('LEFTPADDING',(0,0),(-1,-1), 5),('RIGHTPADDING',(0,0),(-1,-1), 5),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
        ('LINEABOVE',(0,-3),(-1,-3), 1, BLUE),
        ('BACKGROUND',(0,-3),(-1,-1), BLUE_LIGHT),
    ]))
    story.append(sum_t)
    story.append(Paragraph(
        '* Powered by Financial Formulation of Internal Rate of Return (IRR) '
        '& Mathematical Algorithm Newton\'s method', s['tiny']))

    story += disclaimer_block(s)

    # ── Page 2 ─────────────────────────────────────────────────
    story.append(PageBreak())
    story += letter_header(s, name, addr,
        'INVESTMENT ACCOUNT STATEMENT', today.strftime('%d-%m-%Y'),
        stmt_type='Annually', stmt_period=statement_period, page_info='2 of 2')

    # Principal transactions
    if cashflows:
        cf_rows = []
        running = 0.0
        for cf in cashflows:
            u   = float(cf.get('units',0))
            amt = float(cf.get('amount',0))
            nta_d = cf.get('nta_at_date')
            running += u
            cf_rows.append([
                fd(cf.get('date')),
                cf.get('description','—'),
                fm(abs(amt)) + (f'\n@ {fm(nta_d)}' if nta_d else '') if amt else '—',
                fm(nta_d) if nta_d else '—',
                fn(abs(u),4) if u else '—',
                fn(running,4) if running > 0 else '—',
            ])
        story += sec_title(s, 'Principal Transaction')
        story.append(data_tbl(s,
            headers=['Date','Description','Cashflow @ Price','Avg. Cost (RM)','Units Issued','Units Balanced'],
            rows=cf_rows, col_w=[22*mm,48*mm,35*mm,24*mm,22*mm,22*mm]))

    # Distribution history
    if dist_history:
        dh_rows = []
        for d in dist_history:
            dh_rows.append([
                fd(d.get('pmt_date') or d.get('ex_date')),
                d.get('title','—'),
                fn(d.get('dps_sen'),2),
                fn(d.get('units_at_ex_date'),4) if d.get('units_at_ex_date') else '—',
                fm(d.get('amount')) if d.get('amount') else '—',
                fm(d.get('amount')) if d.get('amount') else '—',
            ])
        story += sec_title(s, 'Distribution Transaction')
        story.append(data_tbl(s,
            headers=['Date','Description','DPS (sen)','Holding Units','Distribution Amount','Balanced (RM)'],
            rows=dh_rows, col_w=[22*mm,48*mm,20*mm,28*mm,28*mm,22*mm]))

    story += disclaimer_block(s)
    doc.build(story)
    return buf.getvalue()
