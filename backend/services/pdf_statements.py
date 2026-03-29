"""
ZY-Invest PDF Statement Generator  v2.0.0
Professional layout matching sample factsheet design.
"""
import io, os
from datetime import date, datetime
from decimal import Decimal

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, Image, KeepTogether, PageBreak
)
from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT
from reportlab.platypus import Flowable
from reportlab.lib.utils import ImageReader

# ── Brand ──────────────────────────────────────────────────────
BLUE       = colors.HexColor('#1565C0')
BLUE_LIGHT = colors.HexColor('#EBF4FF')
BLUE_MED   = colors.HexColor('#1976D2')
GREEN      = colors.HexColor('#2E7D32')
ORANGE     = colors.HexColor('#E65100')
RED        = colors.HexColor('#C62828')
GRAY1      = colors.HexColor('#1F2937')
GRAY2      = colors.HexColor('#4B5563')
GRAY3      = colors.HexColor('#9CA3AF')
GRAY4      = colors.HexColor('#F3F4F6')
GRAY5      = colors.HexColor('#E5E7EB')
WHITE      = colors.white

W, H       = A4
LOGO_PATH  = os.path.join(os.path.dirname(__file__), '../../assets/img/logo_zy.png')

# ── Helpers ────────────────────────────────────────────────────
def fm(v, dp=2):
    if v is None: return '—'
    try: return f"RM {float(v):,.{dp}f}"
    except: return '—'

def fn(v, dp=4):
    if v is None: return '—'
    try: return f"{float(v):,.{dp}f}"
    except: return '—'

def fd(v):
    if v is None: return '—'
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
    def ps(name, **kw):
        return ParagraphStyle(name, **kw)
    return {
        'h1':    ps('h1', fontName='Helvetica-Bold', fontSize=14, textColor=GRAY1, spaceAfter=2),
        'h2':    ps('h2', fontName='Helvetica-Bold', fontSize=11, textColor=GRAY1, spaceAfter=2),
        'h3':    ps('h3', fontName='Helvetica-Bold', fontSize=9,  textColor=BLUE,  spaceBefore=8, spaceAfter=3),
        'body':  ps('body', fontName='Helvetica', fontSize=8.5, textColor=GRAY1, leading=13),
        'small': ps('small', fontName='Helvetica', fontSize=7.5, textColor=GRAY2, leading=11),
        'tiny':  ps('tiny',  fontName='Helvetica', fontSize=7,   textColor=GRAY3, leading=10),
        'right': ps('right', fontName='Helvetica', fontSize=8.5, textColor=GRAY1, alignment=TA_RIGHT),
        'right_b': ps('right_b', fontName='Helvetica-Bold', fontSize=8.5, textColor=GRAY1, alignment=TA_RIGHT),
        'bold':  ps('bold', fontName='Helvetica-Bold', fontSize=8.5, textColor=GRAY1),
        'blue_b': ps('blue_b', fontName='Helvetica-Bold', fontSize=8.5, textColor=BLUE),
        'center': ps('center', fontName='Helvetica', fontSize=8.5, textColor=GRAY1, alignment=TA_CENTER),
        'notice': ps('notice', fontName='Helvetica', fontSize=7.5, textColor=GRAY2, leading=11),
        'green_b': ps('green_b', fontName='Helvetica-Bold', fontSize=8.5, textColor=GREEN),
        'red_b':   ps('red_b',   fontName='Helvetica-Bold', fontSize=8.5, textColor=RED),
    }

# ── Chart helpers ──────────────────────────────────────────────
def pie_chart_img(labels, values, size=(55*mm, 55*mm)):
    """Donut pie chart → ReportLab Image."""
    CHART_COLORS = ['#1565C0','#2E7D32','#E65100','#7B1FA2','#00838F','#BF360C','#4527A0','#558B2F']
    fig, ax = plt.subplots(figsize=(2.5, 2.5), dpi=150)
    wedges, _ = ax.pie(values,
        colors=[CHART_COLORS[i % len(CHART_COLORS)] for i in range(len(values))],
        startangle=90, wedgeprops=dict(width=0.55, edgecolor='white', linewidth=1.5))
    ax.set_aspect('equal')
    plt.tight_layout(pad=0.1)
    buf = io.BytesIO()
    fig.savefig(buf, format='png', transparent=True, bbox_inches='tight', dpi=150)
    plt.close(fig)
    buf.seek(0)
    return Image(buf, width=size[0], height=size[1])


def bar_chart_img(labels, values, color='#1565C0', ylabel='NTA (RM)', width=160*mm, height=55*mm):
    """Line chart for NTA history → ReportLab Image."""
    fig, ax = plt.subplots(figsize=(width/mm/3.78, height/mm/3.78), dpi=150)
    x = range(len(labels))
    ax.plot(list(x), values, color=color, linewidth=1.5, zorder=3)
    ax.fill_between(list(x), values, alpha=0.08, color=color)
    ax.set_xticks(list(x)[::max(1, len(x)//8)])
    ax.set_xticklabels([labels[i] for i in range(0, len(labels), max(1, len(labels)//8))],
        fontsize=6, rotation=30, ha='right')
    ax.tick_params(axis='y', labelsize=6)
    ax.grid(axis='y', alpha=0.3, linewidth=0.5)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.set_ylabel(ylabel, fontsize=6)
    plt.tight_layout(pad=0.3)
    buf = io.BytesIO()
    fig.savefig(buf, format='png', transparent=True, bbox_inches='tight', dpi=150)
    plt.close(fig)
    buf.seek(0)
    return Image(buf, width=width, height=height)


def sector_bar_img(sectors, pcts, width=160*mm, height=45*mm):
    """Horizontal bar chart for sector allocation."""
    CHART_COLORS = ['#1565C0','#2E7D32','#E65100','#7B1FA2','#00838F','#BF360C','#4527A0','#558B2F']
    fig, ax = plt.subplots(figsize=(width/mm/3.78, height/mm/3.78), dpi=150)
    y = range(len(sectors))
    bars = ax.barh(list(y), pcts,
        color=[CHART_COLORS[i % len(CHART_COLORS)] for i in range(len(sectors))],
        height=0.6, edgecolor='white')
    ax.set_yticks(list(y))
    ax.set_yticklabels(sectors, fontsize=7)
    ax.set_xlabel('% of NAV', fontsize=6)
    ax.tick_params(axis='x', labelsize=6)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    for bar, pct in zip(bars, pcts):
        ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2,
            f'{pct:.1f}%', va='center', fontsize=6)
    plt.tight_layout(pad=0.3)
    buf = io.BytesIO()
    fig.savefig(buf, format='png', transparent=True, bbox_inches='tight', dpi=150)
    plt.close(fig)
    buf.seek(0)
    return Image(buf, width=width, height=height)


# ── Shared layout blocks ────────────────────────────────────────
def header_block(story, s, title, issued_date, investor_name='',
                 address_lines='', page_info='1 of 1',
                 stmt_type='', stmt_period=''):
    """Full-width header with logo left, title right."""
    # Logo
    logo_w, logo_h = 28*mm, 22*mm
    if os.path.exists(LOGO_PATH):
        try:
            logo_img = Image(LOGO_PATH, width=logo_w, height=logo_h)
        except:
            logo_img = Paragraph('<b>ZY-Invest</b>', s['h1'])
    else:
        logo_img = Paragraph('<b>ZY-Invest</b>', s['h1'])

    # Right side: title + meta
    right_inner = [[Paragraph(title, ParagraphStyle('ht',
        fontName='Helvetica-Bold', fontSize=13, textColor=GRAY1, alignment=TA_RIGHT))]]
    if stmt_type:
        right_inner.append([Paragraph(stmt_type, ParagraphStyle('hst',
            fontName='Helvetica', fontSize=8, textColor=GRAY2, alignment=TA_RIGHT))])

    right_t = Table(right_inner, colWidths=[130*mm])
    right_t.setStyle(TableStyle([
        ('ALIGN',  (0,0),(-1,-1),'RIGHT'),
        ('VALIGN', (0,0),(-1,-1),'MIDDLE'),
        ('TOPPADDING',(0,0),(-1,-1),1),
        ('BOTTOMPADDING',(0,0),(-1,-1),1),
    ]))

    hdr = Table([[logo_img, right_t]], colWidths=[38*mm, 130*mm])
    hdr.setStyle(TableStyle([
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
        ('LEFTPADDING',(0,0),(-1,-1),0),
        ('RIGHTPADDING',(0,0),(-1,-1),0),
    ]))
    story.append(hdr)
    story.append(HRFlowable(width='100%', thickness=2, color=BLUE, spaceAfter=4))

    # Address + meta row (only for personal statements)
    if investor_name:
        meta = [
            ['Page No.',         f': {page_info}'],
            ['Issued Date',      f': {issued_date}'],
        ]
        if stmt_type:
            meta.append(['Statement Type', f': {stmt_type}'])
        if stmt_period:
            meta.append(['Statement Period', f': {stmt_period}'])
        meta += [
            ['Email Address', ': nzy.invest@gmail.com'],
            ['Telephone No.', ': (+60)11 - 1121 8085'],
        ]
        meta_t = Table(
            [[Paragraph(r[0], s['small']), Paragraph(r[1], s['small'])] for r in meta],
            colWidths=[32*mm, 80*mm])
        meta_t.setStyle(TableStyle([
            ('TOPPADDING',(0,0),(-1,-1),1.5),
            ('BOTTOMPADDING',(0,0),(-1,-1),1.5),
            ('LEFTPADDING',(0,0),(-1,-1),0),
        ]))

        addr_lines_p = [Paragraph(f'<b>{investor_name.upper()}</b>',
            ParagraphStyle('an', fontName='Helvetica-Bold', fontSize=9, textColor=GRAY1))]
        for ln in address_lines.split('\n'):
            if ln.strip():
                addr_lines_p.append(Paragraph(ln.strip(), s['small']))

        addr_t = Table([[addr_lines_p, meta_t]], colWidths=[75*mm, 113*mm])
        addr_t.setStyle(TableStyle([
            ('VALIGN',(0,0),(-1,-1),'TOP'),
            ('LEFTPADDING',(0,0),(-1,-1),0),
            ('RIGHTPADDING',(0,0),(-1,-1),0),
            ('TOPPADDING',(0,0),(-1,-1),4),
        ]))
        story.append(addr_t)
        story.append(Spacer(1, 3*mm))


def section_title(story, s, text):
    story.append(Paragraph(text, s['h3']))
    story.append(HRFlowable(width='100%', thickness=0.6, color=BLUE, spaceAfter=3))


def kv_table(story, s, rows, col_w=None):
    """Two-column key-value table."""
    col_w = col_w or [45*mm, 120*mm]
    data = [[Paragraph(k, s['small']), Paragraph(str(v) if v else '—', s['bold'])]
            for k, v in rows]
    t = Table(data, colWidths=col_w)
    t.setStyle(TableStyle([
        ('ROWBACKGROUNDS',(0,0),(-1,-1),[WHITE, GRAY4]),
        ('GRID',(0,0),(-1,-1),0.4,GRAY5),
        ('TOPPADDING',(0,0),(-1,-1),4),
        ('BOTTOMPADDING',(0,0),(-1,-1),4),
        ('LEFTPADDING',(0,0),(-1,-1),6),
        ('RIGHTPADDING',(0,0),(-1,-1),6),
        ('FONTSIZE',(0,0),(-1,-1),8),
        ('TEXTCOLOR',(0,0),(0,-1),GRAY2),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
    ]))
    story.append(t)
    story.append(Spacer(1, 3*mm))


def data_table(story, s, headers, rows, col_widths,
               title='', total_row=None, span_first=False):
    """Styled data table with blue header."""
    if title:
        section_title(story, s, title)

    def cell(txt, bold=False, align=TA_LEFT, color=GRAY1):
        return Paragraph(str(txt) if txt is not None else '—',
            ParagraphStyle('c', fontName='Helvetica-Bold' if bold else 'Helvetica',
                fontSize=8, textColor=color, alignment=align, leading=11))

    hrow = [Paragraph(h, ParagraphStyle('th', fontName='Helvetica-Bold',
        fontSize=8, textColor=WHITE, alignment=TA_CENTER)) for h in headers]

    tdata = [hrow]
    for row in rows:
        tdata.append([cell(c) if not isinstance(c, Paragraph) else c for c in row])
    if total_row:
        tdata.append([cell(c, bold=True) if not isinstance(c, Paragraph) else c
                      for c in total_row])

    t = Table(tdata, colWidths=col_widths, repeatRows=1)
    sty = [
        ('BACKGROUND', (0,0),(-1,0), BLUE),
        ('ROWBACKGROUNDS',(0,1),(-1,-1 if not total_row else -2),[WHITE,GRAY4]),
        ('GRID',(0,0),(-1,-1),0.4,GRAY5),
        ('TOPPADDING',(0,0),(-1,-1),4),
        ('BOTTOMPADDING',(0,0),(-1,-1),4),
        ('LEFTPADDING',(0,0),(-1,-1),5),
        ('RIGHTPADDING',(0,0),(-1,-1),5),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
    ]
    if total_row:
        sty += [
            ('BACKGROUND',(0,-1),(-1,-1),BLUE_LIGHT),
            ('LINEABOVE',(0,-1),(-1,-1),1,BLUE),
        ]
    t.setStyle(TableStyle(sty))
    story.append(t)
    story.append(Spacer(1, 4*mm))


def footer_block(story, s):
    story.append(Spacer(1, 3*mm))
    story.append(HRFlowable(width='100%', thickness=0.5, color=GRAY5, spaceAfter=3))
    story.append(Paragraph('<b>IMPORTANT NOTICES</b>',
        ParagraphStyle('nt', fontName='Helvetica-Bold', fontSize=8, textColor=GRAY1)))
    notices = [
        "<b>1. Confidentiality:</b> This statement contains personal data and is intended solely "
        "for the recipient. Please do not share this document with any third parties.",
        "<b>2. Discrepancies:</b> Please review all figures carefully. Any discrepancies must be "
        "reported to us immediately; failure to do so may result in the recipient bearing any losses.",
        "<b>3. Digital Statements:</b> Effective 1st January 2026, all future portfolio statements "
        "will be provided exclusively via WhatsApp.",
    ]
    for n in notices:
        story.append(Paragraph(n, s['notice']))
    story.append(Spacer(1, 2*mm))
    ft = Table([[
        Paragraph('Head Office: None', s['tiny']),
        Paragraph('Line: (60)11-1121 8085  |  Email: nzy.invest@gmail.com', s['tiny']),
    ]], colWidths=[45*mm, 123*mm])
    ft.setStyle(TableStyle([
        ('ALIGN',(1,0),(1,0),'RIGHT'),
        ('TOPPADDING',(0,0),(-1,-1),0),
    ]))
    story.append(ft)


# ══════════════════════════════════════════════════════════════
#  1.  FACTSHEET
# ══════════════════════════════════════════════════════════════
def generate_factsheet(fund_data: dict, holdings: list, performance: dict,
                       distributions: list, nta_history: list,
                       sector_data: list, manager_comment: str = '') -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
        leftMargin=15*mm, rightMargin=15*mm,
        topMargin=12*mm, bottomMargin=12*mm)
    s = S()
    story = []
    today = date.today()
    period = today.strftime('%B %Y')

    header_block(story, s,
        title=f"ZY Family Vision Portfolio  —  {period}",
        issued_date=today.strftime('%d-%m-%Y'))

    story.append(Paragraph(
        "The portfolio aims to provide our investors with capital appreciation higher than the "
        "prevailing fixed-deposit rate by investing in a high-growth portfolio of stocks and "
        "fixed income instruments.", s['body']))
    story.append(Spacer(1, 4*mm))

    # ── Two-column: Fund Details + Sector Pie ──────────────────
    nta   = fund_data.get('current_nta', 0)
    aum   = fund_data.get('aum', 0)
    units = fund_data.get('total_units', 0)

    fd_rows = [
        ['Manager',               'Mr. Ng Zhi Yao'],
        ['Fund Category',         'Equity Fund'],
        ['Launch Date',           '13 December 2021'],
        ['Unit Net Asset Value',  fm(nta, 4)],
        ['Fund Size',             fm(aum)],
        ['Units in Circulation',  fn(units, 2) + ' units'],
        ['Financial Year End',    '30 November'],
        ['Min. Initial Investment','RM 10,000.00'],
        ['Min. Additional',       'RM 1,000.00'],
        ['Benchmark',             'FBMKLCI, Tech Index'],
        ['Annual Mgmt Fee',       '0.10% p.a. of NAV'],
        ['Performance Fee',       '10.0% p.a. of excess return'],
        ['Distribution Policy',   'At least 80% of gross income'],
    ]
    fd_data = [[Paragraph(k, s['small']), Paragraph(v, s['bold'])] for k,v in fd_rows]
    fd_t = Table(fd_data, colWidths=[38*mm, 55*mm])
    fd_t.setStyle(TableStyle([
        ('ROWBACKGROUNDS',(0,0),(-1,-1),[WHITE,GRAY4]),
        ('GRID',(0,0),(-1,-1),0.4,GRAY5),
        ('TOPPADDING',(0,0),(-1,-1),3.5),
        ('BOTTOMPADDING',(0,0),(-1,-1),3.5),
        ('LEFTPADDING',(0,0),(-1,-1),5),
        ('FONTSIZE',(0,0),(-1,-1),7.5),
        ('TEXTCOLOR',(0,0),(0,-1),GRAY2),
    ]))

    # Sector pie chart
    pie_img = None
    if sector_data:
        lbls = [r.get('asset_class') or r.get('sector','') for r in sector_data[:6]]
        vals = [max(float(r.get('weight_pct',0)),0.01) for r in sector_data[:6]]
        if sum(vals) > 0:
            pie_img = pie_chart_img(lbls, vals, size=(55*mm, 55*mm))

    left_block = [
        [Paragraph('Fund Details', s['h3'])],
        [fd_t],
    ]
    left_t = Table(left_block, colWidths=[95*mm])
    left_t.setStyle(TableStyle([
        ('TOPPADDING',(0,0),(-1,-1),0),
        ('BOTTOMPADDING',(0,0),(-1,-1),2),
        ('LEFTPADDING',(0,0),(-1,-1),0),
    ]))

    right_block_data = []
    right_block_data.append([Paragraph('Sector Allocation*', s['h3'])])
    if pie_img:
        # Legend
        CHART_COLORS = ['#1565C0','#2E7D32','#E65100','#7B1FA2','#00838F','#BF360C']
        legend_rows = []
        for i, r in enumerate(sector_data[:6]):
            nm  = r.get('asset_class') or r.get('sector','')
            pct = float(r.get('weight_pct',0))
            legend_rows.append([
                Paragraph(f'<font color="{CHART_COLORS[i%6]}">■</font> {nm}', s['tiny']),
                Paragraph(f'{pct:.1f}%', s['tiny']),
            ])
        legend_t = Table(legend_rows, colWidths=[50*mm, 15*mm])
        legend_t.setStyle(TableStyle([
            ('TOPPADDING',(0,0),(-1,-1),1.5),
            ('BOTTOMPADDING',(0,0),(-1,-1),1.5),
            ('LEFTPADDING',(0,0),(-1,-1),2),
        ]))
        chart_legend = Table([[pie_img, legend_t]], colWidths=[58*mm, 68*mm])
        chart_legend.setStyle(TableStyle([
            ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
            ('LEFTPADDING',(0,0),(-1,-1),0),
        ]))
        right_block_data.append([chart_legend])
    else:
        right_block_data.append([Paragraph('No holdings data', s['small'])])

    right_t = Table(right_block_data, colWidths=[63*mm])
    right_t.setStyle(TableStyle([
        ('TOPPADDING',(0,0),(-1,-1),0),
        ('BOTTOMPADDING',(0,0),(-1,-1),2),
        ('LEFTPADDING',(0,0),(-1,-1),0),
    ]))

    two_col = Table([[left_t, right_t]], colWidths=[100*mm, 68*mm])
    two_col.setStyle(TableStyle([
        ('VALIGN',(0,0),(-1,-1),'TOP'),
        ('LEFTPADDING',(0,0),(-1,-1),0),
        ('RIGHTPADDING',(0,0),(-1,-1),0),
    ]))
    story.append(two_col)
    story.append(Spacer(1, 4*mm))

    # ── Investment Strategy ─────────────────────────────────────
    section_title(story, s, 'Investment Strategy')
    strategy = (
        "The strategic limit on asset allocation of the fund is as follows: "
        "Equities: Minimum 60% and maximum 98%.  "
        "Derivatives: Maximum 5%.  "
        "Fixed-income securities: Minimum 2% and maximum 40%."
    )
    story.append(Paragraph(strategy, s['body']))
    story.append(Spacer(1, 3*mm))

    # ── NTA Performance Chart (full history) ────────────────────
    if nta_history:
        section_title(story, s, 'Portfolio Performance Analysis')
        dates = [r.get('date','') for r in nta_history]
        ntas  = [float(r.get('nta',1)) for r in nta_history]
        # Shorten date labels
        short_dates = []
        for d in dates:
            try:
                dt = datetime.fromisoformat(str(d))
                short_dates.append(dt.strftime('%b-%y'))
            except:
                short_dates.append(str(d)[:7])
        chart = bar_chart_img(short_dates, ntas, width=168*mm, height=55*mm)
        story.append(chart)

        # Performance table
        perf = performance.get('period_returns', {})
        total_ret = fund_data.get('total_return_pct', 0)
        perf_headers = ['', '1 Month', '3 Months', '6 Months', '1 Year', '3 Years', 'All']
        perf_row = ['Portfolio'] + [fp(perf.get(k)) for k in ['1M','3M','6M','1Y','3Y']] + [fp(total_ret)]
        data_table(story, s, headers=perf_headers, rows=[perf_row],
            col_widths=[25*mm,23*mm,23*mm,23*mm,23*mm,23*mm,23*mm])

    # ── Distribution History ────────────────────────────────────
    if distributions:
        section_title(story, s, 'Distribution History')
        dist_rows = []
        for d in distributions:
            pr = d.get('payout_ratio')
            dist_rows.append([
                d.get('financial_year','—'),
                d.get('title','—'),
                fn(d.get('dps_sen'),2) + ' sen',
                f"{float(pr):.1f}%" if pr else '—',
            ])
        data_table(story, s,
            headers=['Year','Description','DPS (sen)','Payout Ratio'],
            rows=dist_rows,
            col_widths=[18*mm, 100*mm, 28*mm, 28*mm])

    # ── Manager's Comments ──────────────────────────────────────
    section_title(story, s, "Manager's Comments")
    comment_text = manager_comment or (
        "Our portfolio maintains its current strategic positions. We continue to monitor market "
        "developments and macroeconomic conditions closely. The fund remains well-positioned "
        "relative to its investment objectives for the current financial year."
    )
    story.append(Paragraph(comment_text, s['body']))
    story.append(Spacer(1, 3*mm))

    # ── Largest Holdings ────────────────────────────────────────
    if holdings:
        section_title(story, s, 'Largest Holdings*')
        h_rows = []
        for h in holdings[:10]:
            pct = h.get('weight_pct') or (float(h.get('mv_portion',0))*100)
            h_rows.append([h.get('instrument','—'), f"{float(pct):.2f}%"])
        data_table(story, s,
            headers=['Asset Name','Percentage'],
            rows=h_rows,
            col_widths=[140*mm, 28*mm])
        story.append(Paragraph(
            '* As percentage of Net Asset Value (NAV) of the fund', s['tiny']))
        story.append(Spacer(1, 2*mm))

    # ── Disclaimer ──────────────────────────────────────────────
    story.append(HRFlowable(width='100%', thickness=0.5, color=GRAY5))
    story.append(Spacer(1, 2*mm))
    story.append(Paragraph('<b>Disclaimer</b>',
        ParagraphStyle('dis', fontName='Helvetica-Bold', fontSize=8, textColor=GRAY1)))
    story.append(Paragraph(
        "Investment involves significant risk, including the potential loss of principal. "
        "Past performance is not indicative of future results, and market volatility may affect "
        "the timing of realized gains. Please note that this portfolio update is intended strictly "
        "for family members only; it does not constitute a financial prospectus and is not open "
        "to the general public or external investors.", s['notice']))
    story.append(Spacer(1, 3*mm))
    ft = Table([[
        Paragraph('Head Office: None', s['tiny']),
        Paragraph('Line: (60)11-1121 8085  |  Email: nzy.invest@gmail.com', s['tiny']),
    ]], colWidths=[45*mm, 123*mm])
    ft.setStyle(TableStyle([('ALIGN',(1,0),(1,0),'RIGHT'),('TOPPADDING',(0,0),(-1,-1),0)]))
    story.append(ft)

    doc.build(story)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════
#  2.  SUBSCRIPTION STATEMENT
# ══════════════════════════════════════════════════════════════
def generate_subscription(investor: dict, cashflows: list,
                           statement_period: str = '') -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
        leftMargin=15*mm, rightMargin=15*mm,
        topMargin=12*mm, bottomMargin=12*mm)
    s  = S()
    story = []
    today = date.today()
    name  = investor.get('name','')
    addr  = '\n'.join(filter(None,[
        investor.get('address_line1',''),
        investor.get('address_line2',''),
        ' '.join(filter(None,[investor.get('postcode',''),investor.get('city','')])),
        investor.get('state',''),
    ]))

    header_block(story, s,
        title='FUND SUBSCRIPTION STATEMENT',
        issued_date=today.strftime('%d-%m-%Y'),
        stmt_type='Transaction Statement',
        stmt_period=statement_period or today.strftime('%d/%m/%Y'),
        investor_name=name,
        address_lines=addr)

    # Investor info grid
    section_title(story, s, "Investor's Information")
    inv_fields = [
        ['Account Type', investor.get('account_type','Nominee Account'),
         'Account ID',   investor.get('account_id','—')],
        ['Registered Name', name,
         'Settlement Type', 'Banking'],
        ['Phone No.',    investor.get('phone','—'),
         'Bank Name',    investor.get('bank_name','—')],
        ['Email Address', investor.get('email','—'),
         'Bank Account No.', investor.get('bank_account_no','—')],
        ['Nominee Name', '—',
         'Total Days Held', f"{investor.get('days_held',0)} days"],
    ]
    inv_data = []
    for row in inv_fields:
        inv_data.append([
            Paragraph(row[0], s['small']),
            Paragraph(str(row[1]) if row[1] else '—', s['body']),
            Paragraph(row[2], s['small']),
            Paragraph(str(row[3]) if row[3] else '—', s['body']),
        ])
    inv_t = Table(inv_data, colWidths=[32*mm, 56*mm, 32*mm, 48*mm])
    inv_t.setStyle(TableStyle([
        ('GRID',(0,0),(-1,-1),0.4,GRAY5),
        ('ROWBACKGROUNDS',(0,0),(-1,-1),[WHITE,GRAY4]),
        ('TOPPADDING',(0,0),(-1,-1),5),
        ('BOTTOMPADDING',(0,0),(-1,-1),5),
        ('LEFTPADDING',(0,0),(-1,-1),6),
        ('FONTSIZE',(0,0),(-1,-1),8),
        ('TEXTCOLOR',(0,0),(0,-1),GRAY2),
        ('TEXTCOLOR',(2,0),(2,-1),GRAY2),
    ]))
    story.append(inv_t)
    story.append(Spacer(1, 5*mm))

    # Principal transactions
    if cashflows:
        cf_rows = []
        for cf in cashflows:
            amt = float(cf.get('amount',0))
            cf_rows.append([
                fd(cf.get('date')),
                cf.get('description','—'),
                fm(abs(amt)) if amt != 0 else '—',
                fn(cf.get('nta_at_date'),4) if cf.get('nta_at_date') else '—',
                fn(cf.get('units'),4) if cf.get('units') else '—',
                fn(cf.get('avg_cost'),4) if cf.get('avg_cost') else '—',
            ])

        # Add Opening row at top, Closing at bottom
        total_inv = sum(abs(float(cf.get('amount',0))) for cf in cashflows if float(cf.get('amount',0)) > 0)
        final_units = sum(float(cf.get('units',0)) for cf in cashflows)
        avg_c = cashflows[-1].get('nta_at_date') if cashflows else None

        all_rows = [
            [fd(cashflows[0].get('date')), 'Opening','—','—','—','—'],
            *cf_rows,
            [fd(cashflows[-1].get('date')), 'Closing',
             fm(total_inv),'—', fn(final_units,4), fn(avg_c,4) if avg_c else '—'],
        ]
        data_table(story, s,
            title='Principal Transaction',
            headers=['Date','Description','Investment Value','Subscription Price',
                     'Unit Balanced','Average Cost'],
            rows=all_rows,
            col_widths=[24*mm,38*mm,30*mm,28*mm,26*mm,22*mm])

    footer_block(story, s)
    doc.build(story)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════
#  3.  DIVIDEND PAYMENT STATEMENT
# ══════════════════════════════════════════════════════════════
def generate_dividend_statement(investor: dict, distributions: list,
                                 financial_year: str) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
        leftMargin=15*mm, rightMargin=15*mm,
        topMargin=12*mm, bottomMargin=12*mm)
    s = S()
    story = []
    today = date.today()
    name  = investor.get('name','')
    addr  = '\n'.join(filter(None,[
        investor.get('address_line1',''),
        investor.get('address_line2',''),
        ' '.join(filter(None,[investor.get('postcode',''),investor.get('city','')])),
        investor.get('state',''),
    ]))

    pmt_period = fd(distributions[0].get('pmt_date')) if distributions else today.strftime('%d/%m/%Y')

    header_block(story, s,
        title='DIVIDEND PAYMENT STATEMENT',
        issued_date=today.strftime('%d-%m-%Y'),
        stmt_type=financial_year,
        stmt_period=pmt_period,
        investor_name=name,
        address_lines=addr)

    # Investor info
    section_title(story, s, "Investor's Information")
    inv_fields = [
        ['Investor\'s Name', name,          'Settlement Type',   'Banking'],
        ['Phone Number',     investor.get('phone','—'),
         'Bank Name',        investor.get('bank_name','—')],
        ['Email Address',    investor.get('email','—'),
         'Bank Account No.', investor.get('bank_account_no','—')],
    ]
    inv_data = []
    for row in inv_fields:
        inv_data.append([
            Paragraph(row[0], s['small']),
            Paragraph(str(row[1]) if row[1] else '—', s['body']),
            Paragraph(row[2], s['small']),
            Paragraph(str(row[3]) if row[3] else '—', s['body']),
        ])
    inv_t = Table(inv_data, colWidths=[32*mm, 56*mm, 32*mm, 48*mm])
    inv_t.setStyle(TableStyle([
        ('GRID',(0,0),(-1,-1),0.4,GRAY5),
        ('ROWBACKGROUNDS',(0,0),(-1,-1),[WHITE,GRAY4]),
        ('TOPPADDING',(0,0),(-1,-1),5),
        ('BOTTOMPADDING',(0,0),(-1,-1),5),
        ('LEFTPADDING',(0,0),(-1,-1),6),
        ('FONTSIZE',(0,0),(-1,-1),8),
        ('TEXTCOLOR',(0,0),(0,-1),GRAY2),
        ('TEXTCOLOR',(2,0),(2,-1),GRAY2),
    ]))
    story.append(inv_t)
    story.append(Spacer(1, 5*mm))

    # Dividend details table
    if distributions:
        total_dps = total_amt = 0.0
        rows = []
        for d in distributions:
            dps  = float(d.get('dps_sen',0))
            units= float(d.get('units_at_ex_date',0))
            amt  = float(d.get('amount',0))
            eps  = d.get('eps')
            dpr  = d.get('payout_ratio')
            total_dps += dps
            total_amt += amt
            rows.append([
                fd(d.get('pmt_date') or d.get('ex_date')),
                d.get('title', d.get('dist_type','')).replace('_',' ').title(),
                fn(units, 4),
                fn(eps, 2) if eps else '—',
                f"{float(dpr):.1f}%" if dpr else '—',
                fn(dps, 2),
                Paragraph(f'<b>{fm(amt)}</b>', ParagraphStyle('amt',
                    fontName='Helvetica-Bold', fontSize=8, textColor=BLUE, alignment=TA_RIGHT)),
            ])

        data_table(story, s,
            title='Dividend Details',
            headers=['Date','Description','Holding Units','EPS','DPR','DPS (sen)','Dividend Amount'],
            rows=rows,
            col_widths=[24*mm,38*mm,26*mm,16*mm,16*mm,18*mm,30*mm],
            total_row=['','Total','','','', fn(total_dps,2),
                Paragraph(f'<b>{fm(total_amt)}</b>',
                    ParagraphStyle('ta', fontName='Helvetica-Bold', fontSize=8,
                        textColor=BLUE, alignment=TA_RIGHT))])
        story.append(Paragraph(
            'Notes: EPS = Earning Per Share  |  DPR = Dividend Payout Ratio  |  DPS = Dividend Per Share (sen)',
            s['tiny']))
        story.append(Spacer(1, 2*mm))

    footer_block(story, s)
    doc.build(story)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════
#  4.  INVESTMENT ACCOUNT STATEMENT (Annual)
# ══════════════════════════════════════════════════════════════
def generate_account_statement(investor: dict, summary: dict,
                                cashflows: list, dist_history: list,
                                statement_period: str,
                                financial_year: str) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
        leftMargin=15*mm, rightMargin=15*mm,
        topMargin=12*mm, bottomMargin=12*mm)
    s = S()
    story = []
    today = date.today()
    name  = investor.get('name','')
    addr  = '\n'.join(filter(None,[
        investor.get('address_line1',''),
        investor.get('address_line2',''),
        ' '.join(filter(None,[investor.get('postcode',''),investor.get('city','')])),
        investor.get('state',''),
    ]))

    def make_header(page_n):
        header_block(story, s,
            title='INVESTMENT ACCOUNT STATEMENT',
            issued_date=today.strftime('%d-%m-%Y'),
            stmt_type='Annually',
            stmt_period=statement_period,
            investor_name=name,
            address_lines=addr)

    # ── PAGE 1 ──────────────────────────────────────────────────
    make_header(1)
    story.append(Paragraph(f'Page 1 of 2', ParagraphStyle('pg',
        fontName='Helvetica', fontSize=7, textColor=GRAY3, alignment=TA_RIGHT)))

    # Investor info
    section_title(story, s, "Investor's Information")
    inv_fields = [
        ['Account Type',    summary.get('account_type','Nominee Account'),
         'Account ID',      investor.get('account_id','—')],
        ['Registered Name', name,
         'Settlement Type', 'Banking'],
        ['Phone No.',       investor.get('phone','—'),
         'Bank Name',       investor.get('bank_name','—')],
        ['Email Address',   investor.get('email','—'),
         'Bank Account No.',investor.get('bank_account_no','—')],
        ['Nominee Name','—','Total Days Held', f"{summary.get('days_held',0)} days"],
    ]
    inv_data = []
    for row in inv_fields:
        inv_data.append([
            Paragraph(row[0], s['small']),
            Paragraph(str(row[1]) if row[1] else '—', s['body']),
            Paragraph(row[2], s['small']),
            Paragraph(str(row[3]) if row[3] else '—', s['body']),
        ])
    inv_t = Table(inv_data, colWidths=[32*mm, 56*mm, 32*mm, 48*mm])
    inv_t.setStyle(TableStyle([
        ('GRID',(0,0),(-1,-1),0.4,GRAY5),
        ('ROWBACKGROUNDS',(0,0),(-1,-1),[WHITE,GRAY4]),
        ('TOPPADDING',(0,0),(-1,-1),5),
        ('BOTTOMPADDING',(0,0),(-1,-1),5),
        ('LEFTPADDING',(0,0),(-1,-1),6),
        ('FONTSIZE',(0,0),(-1,-1),8),
        ('TEXTCOLOR',(0,0),(0,-1),GRAY2),
        ('TEXTCOLOR',(2,0),(2,-1),GRAY2),
    ]))
    story.append(inv_t)
    story.append(Spacer(1, 5*mm))

    # Account summary
    nta_v    = float(summary.get('current_nta',0))
    units_v  = float(summary.get('units',0))
    costs_v  = float(summary.get('total_costs',0))
    mv_v     = float(summary.get('market_value',0))
    unreal_v = float(summary.get('unrealized_pl',0))
    real_v   = float(summary.get('realized_pl',0))
    div_v    = float(summary.get('dividends_received',0))
    adj_v    = float(summary.get('adjustment',0))
    total_pl = unreal_v + real_v + div_v + adj_v
    total_pct= (total_pl / abs(costs_v) * 100) if costs_v else 0
    irr      = summary.get('irr')
    avg_cost = costs_v / units_v if units_v else 0

    def pl_p(v):
        if v is None or v == 0: return Paragraph('—', s['body'])
        color = GREEN if float(v) >= 0 else RED
        return Paragraph(f'<b>{fm(v)}</b>',
            ParagraphStyle('pl', fontName='Helvetica-Bold', fontSize=8,
                textColor=color, alignment=TA_RIGHT))

    section_title(story, s, 'Account Summary')
    sum_data = [
        ['Fields','Holding Units','Average Price','Total Value (RM)'],
        ['(a)  Latest Fund Price',  fn(units_v,4), fn(nta_v,4),   fm(mv_v)],
        ['(b)  Subscription Cost',  fn(units_v,4), fn(avg_cost,4), f'({fm(costs_v)})'],
        ['(c)  Unrealized Profit & Loss:  (a) + (b)', '', '',      fm(unreal_v)],
        ['(d)  Realized Profit & Loss',  '', '',
         fm(real_v) if real_v else '—'],
        ['(e)  Dividend Received', '', '',
         fm(div_v) if div_v else '—'],
        ['(f)  Adjustment: Initial Capital', '', '',
         f'({fm(abs(adj_v))})' if adj_v < 0 else (fm(adj_v) if adj_v else '—')],
    ]
    sum_tdata = [[Paragraph(h, ParagraphStyle('sh', fontName='Helvetica-Bold',
        fontSize=8, textColor=WHITE, alignment=TA_CENTER)) for h in sum_data[0]]]
    for row in sum_data[1:]:
        sum_tdata.append([
            Paragraph(row[0], s['body']),
            Paragraph(row[1], ParagraphStyle('rc', fontName='Helvetica', fontSize=8,
                textColor=GRAY1, alignment=TA_RIGHT)),
            Paragraph(row[2], ParagraphStyle('rc2', fontName='Helvetica', fontSize=8,
                textColor=GRAY1, alignment=TA_RIGHT)),
            Paragraph(row[3], ParagraphStyle('rv', fontName='Helvetica', fontSize=8,
                textColor=GRAY1, alignment=TA_RIGHT)),
        ])

    # Total rows
    def bold_right(txt, color=GRAY1):
        return Paragraph(f'<b>{txt}</b>',
            ParagraphStyle('br', fontName='Helvetica-Bold', fontSize=8,
                textColor=color, alignment=TA_RIGHT))

    def bold_left(txt):
        return Paragraph(f'<b>{txt}</b>',
            ParagraphStyle('bl', fontName='Helvetica-Bold', fontSize=8, textColor=GRAY1))

    tpl_color = GREEN if total_pl >= 0 else RED
    tp_color  = GREEN if total_pct >= 0 else RED
    irr_color = GREEN if irr and float(irr) >= 0 else RED

    sum_tdata.append([bold_left('Total Profit & Loss:  (c) + (d) + (e) + (f)'),
        Paragraph('', s['body']), Paragraph('', s['body']),
        bold_right(fm(total_pl), tpl_color)])
    sum_tdata.append([bold_left('Total Performance %'),
        Paragraph('', s['body']), Paragraph('', s['body']),
        bold_right(fp(total_pct), tp_color)])
    sum_tdata.append([bold_left('Annualised Performance* %'),
        Paragraph('', s['body']), Paragraph('', s['body']),
        bold_right(fp(irr) if irr else '—', irr_color)])

    sum_t = Table(sum_tdata, colWidths=[85*mm, 25*mm, 25*mm, 33*mm])
    sum_sty = [
        ('BACKGROUND',(0,0),(-1,0), BLUE),
        ('ROWBACKGROUNDS',(0,1),(-1,-4),[WHITE,GRAY4]),
        ('GRID',(0,0),(-1,-1),0.4,GRAY5),
        ('TOPPADDING',(0,0),(-1,-1),4),
        ('BOTTOMPADDING',(0,0),(-1,-1),4),
        ('LEFTPADDING',(0,0),(-1,-1),5),
        ('RIGHTPADDING',(0,0),(-1,-1),5),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
        ('LINEABOVE',(0,-3),(-1,-3),1,BLUE),
        ('BACKGROUND',(0,-3),(-1,-1),BLUE_LIGHT),
    ]
    sum_t.setStyle(TableStyle(sum_sty))
    story.append(sum_t)
    story.append(Paragraph(
        '* Powered by Financial Formulation of Internal Rate of Return (IRR) '
        '& Mathematical Algorithm Newton\'s method', s['tiny']))
    story.append(Spacer(1, 2*mm))
    footer_block(story, s)

    # ── PAGE 2 ──────────────────────────────────────────────────
    story.append(PageBreak())
    make_header(2)
    story.append(Paragraph('Page 2 of 2', ParagraphStyle('pg2',
        fontName='Helvetica', fontSize=7, textColor=GRAY3, alignment=TA_RIGHT)))

    # Principal transactions
    if cashflows:
        cf_rows = []
        running_units = 0.0
        for cf in cashflows:
            u   = float(cf.get('units',0))
            amt = float(cf.get('amount',0))
            nta_d = cf.get('nta_at_date')
            running_units += u
            cf_rows.append([
                fd(cf.get('date')),
                cf.get('description','—'),
                (fm(abs(amt)) + (f'\n@ {fm(nta_d)}' if nta_d else '')) if amt else '—',
                fm(nta_d) if nta_d else '—',
                fn(abs(u),4) if u else '—',
                fn(running_units,4) if running_units > 0 else '—',
            ])
        data_table(story, s,
            title='Principal Transaction',
            headers=['Date','Description','Cashflow @ Price','Avg. Cost (RM)','Units Issued','Units Balanced'],
            rows=cf_rows,
            col_widths=[22*mm, 48*mm, 35*mm, 24*mm, 23*mm, 23*mm])

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
        data_table(story, s,
            title='Distribution Transaction',
            headers=['Date','Description','DPS (sen)','Holding Units','Distribution Amount','Balanced (RM)'],
            rows=dh_rows,
            col_widths=[22*mm, 48*mm, 20*mm, 28*mm, 30*mm, 27*mm])

    footer_block(story, s)
    doc.build(story)
    return buf.getvalue()
