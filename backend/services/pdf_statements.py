"""
ZY-Invest PDF Statement Generator  v6.0
Layout decisions:
  - Narrower margins (12mm L/R) for more content width
  - Header: logo left, company name right — drawn on canvas, never overlaps body
  - Body frame cleanly below header HR and above disclaimer+footer zone
  - Letter header: TITLE top-right (large), then NAME/ADDRESS left vs META right — fully polarised
  - Disclaimer + Important Notices drawn on canvas in last-page footer gap
  - Two-pass build to know total pages before drawing disclaimer
  - Per-record generation: each cashflow/distribution row → its own daily PDF
"""
import io, os, copy
from datetime import date, datetime

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (
    BaseDocTemplate, Frame, PageTemplate, PageBreak,
    Paragraph, Spacer, Table, TableStyle, HRFlowable, Image, KeepTogether
)
from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT

# ── Colours ────────────────────────────────────────────────────
BLUE   = colors.HexColor('#1565C0')
BLU_LT = colors.HexColor('#EBF4FF')
GREEN  = colors.HexColor('#2E7D32')
RED    = colors.HexColor('#C62828')
G1     = colors.HexColor('#1F2937')
G2     = colors.HexColor('#4B5563')
G3     = colors.HexColor('#9CA3AF')
G4     = colors.HexColor('#F3F4F6')
G5     = colors.HexColor('#E5E7EB')
WHITE  = colors.white

W, H = A4

# ── Margins (narrower = more content) ─────────────────────────
LM = RM = 12 * mm               # 12mm left/right margins

# ── Header zone (canvas-drawn, above body frame) ───────────────
# Logo: top-left, 8mm from top, 20mm tall
# "ZY-Invest": top-right, aligned with logo
# Blue HR: 29mm from top
HDR_H    = 29 * mm              # total header zone height
HDR_LINE = H - HDR_H            # y of blue HR

# ── Footer zone (canvas-drawn, below body frame) ───────────────
FTR_LINE = 17 * mm              # y of footer HR line
FTR_TEXT = 10 * mm              # y of footer text baseline

# ── Disclaimer zone (last page only, above footer) ─────────────
# 3 notices × ~9mm + title 6mm + spacing = ~36mm
DISC_H   = 38 * mm
DISC_TOP = FTR_LINE + DISC_H    # top of disclaimer area

# ── Body frame ─────────────────────────────────────────────────
BODY_BOT = DISC_TOP + 3 * mm   # 3mm gap above disclaimer
BODY_TOP = HDR_LINE - 4 * mm   # 4mm gap below header HR
BODY_H   = BODY_TOP - BODY_BOT
CW       = W - LM - RM         # ≈ 171mm

LOGO = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logo.png')
CC   = ['#1565C0', '#2E7D32', '#E65100', '#7B1FA2', '#00838F', '#BF360C', '#4527A0', '#558B2F']


# ── Formatters ─────────────────────────────────────────────────
def fm(v, dp=2):
    if v is None: return '—'
    try:    return f"RM {float(v):,.{dp}f}"
    except: return '—'

def fn(v, dp=4):
    if v is None: return '—'
    try:    return f"{float(v):,.{dp}f}"
    except: return '—'

def fd(v):
    if not v: return '—'
    if isinstance(v, (date, datetime)): return v.strftime('%d %b %Y')
    try:    return datetime.fromisoformat(str(v)).strftime('%d %b %Y')
    except: return str(v)

def fp(v):
    if v is None: return '—'
    try:
        f = float(v)
        return f"+{f:.2f}%" if f >= 0 else f"{f:.2f}%"
    except: return '—'


# ── Styles ─────────────────────────────────────────────────────
def S():
    def p(n, **k): return ParagraphStyle(n, **k)
    return {
        'h3':    p('h3',   fontName='Helvetica-Bold', fontSize=9,   textColor=BLUE, spaceBefore=4, spaceAfter=2),
        'body':  p('body', fontName='Helvetica',      fontSize=8.5, textColor=G1,   leading=13),
        'small': p('sm',   fontName='Helvetica',      fontSize=7.5, textColor=G2,   leading=11),
        'tiny':  p('tiny', fontName='Helvetica',      fontSize=7,   textColor=G3,   leading=10),
        'bold':  p('b',    fontName='Helvetica-Bold', fontSize=8.5, textColor=G1),
        'tr':    p('tr',   fontName='Helvetica-Bold', fontSize=13,  textColor=G1,   alignment=TA_RIGHT),
        'notice':p('n',    fontName='Helvetica',      fontSize=7.5, textColor=G2,   leading=11),
    }


# ── Canvas page callback ────────────────────────────────────────
def _page_cb(n_pages):
    """Return an onPage callback that knows total page count."""
    def _draw(canvas, doc):
        canvas.saveState()
        pg = doc.page

        # ── Logo (top-left) ─────────────────────────────────
        logo_y = H - HDR_H + 4*mm
        if os.path.exists(LOGO):
            try:
                canvas.drawImage(LOGO, LM, logo_y,
                                 width=26*mm, height=20*mm,
                                 preserveAspectRatio=True, anchor='sw', mask='auto')
            except Exception:
                canvas.setFont('Helvetica-Bold', 10)
                canvas.setFillColor(BLUE)
                canvas.drawString(LM, logo_y + 8*mm, 'ZY-Invest')

        # ── Blue HR below header ─────────────────────────────
        canvas.setStrokeColor(BLUE)
        canvas.setLineWidth(1.5)
        canvas.line(LM, HDR_LINE, W - RM, HDR_LINE)

        # ── Footer HR ───────────────────────────────────────
        canvas.setStrokeColor(G5)
        canvas.setLineWidth(0.5)
        canvas.line(LM, FTR_LINE, W - RM, FTR_LINE)

        # ── Footer text ─────────────────────────────────────
        canvas.setFont('Helvetica', 6.5)
        canvas.setFillColor(G3)
        canvas.drawString(LM, FTR_TEXT,
            'Head Office: None  |  Line: (60)11-1121 8085  |  '
            'Email: nzy.invest@gmail.com  |  Website: zy-invest.com')
        canvas.drawRightString(W - RM, FTR_TEXT, f'Page {pg}')

        # ── Disclaimer (last page only, drawn in DISC zone) ──
        if pg == n_pages:
            dy = FTR_LINE + 2*mm
            canvas.setStrokeColor(G5)
            canvas.setLineWidth(0.4)
            canvas.line(LM, dy + DISC_H - 4*mm, W - RM, dy + DISC_H - 4*mm)

            canvas.setFont('Helvetica-Bold', 7.5)
            canvas.setFillColor(G1)
            canvas.drawString(LM, dy + DISC_H - 8*mm, 'IMPORTANT NOTICES')

            notices = [
                '1. Confidentiality: This statement contains personal data intended solely for the '
                'recipient. Please do not share this document with any third parties.',
                '2. Discrepancies: Please review all figures carefully. Any discrepancies must be '
                'reported to us immediately; failure to do so may result in the recipient bearing any losses.',
                '3. Digital Statements: Effective 1st January 2026, all future portfolio statements '
                'will be provided exclusively via WhatsApp.',
            ]
            canvas.setFont('Helvetica', 6.8)
            canvas.setFillColor(G2)
            line_h = 3.5*mm
            chars_w = int(CW / (6.8 * 0.53))
            y = dy + DISC_H - 14*mm
            for notice in notices:
                words = notice.split()
                line  = ''
                for w in words:
                    test = (line + ' ' + w).strip()
                    if len(test) > chars_w and line:
                        canvas.drawString(LM, y, line); y -= line_h; line = w
                    else:
                        line = test
                if line:
                    canvas.drawString(LM, y, line)
                y -= line_h + 2*mm

        canvas.restoreState()
    return _draw


# ── Two-pass build ──────────────────────────────────────────────
def _count_pages(story):
    class _C(BaseDocTemplate):
        def __init__(self):
            super().__init__(io.BytesIO(), pagesize=A4,
                             leftMargin=LM, rightMargin=RM,
                             topMargin=H - BODY_TOP, bottomMargin=BODY_BOT)
            fr = Frame(LM, BODY_BOT, CW, BODY_H, id='b',
                       leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0)
            self.addPageTemplates([PageTemplate(id='m', frames=[fr])])
            self._n = 0
        def handle_pageEnd(self):
            self._n += 1; super().handle_pageEnd()
    c = _C(); c.build(copy.deepcopy(story)); return max(c._n, 1)


def _build(story):
    n   = _count_pages(story)
    buf = io.BytesIO()
    doc = BaseDocTemplate(buf, pagesize=A4,
                          leftMargin=LM, rightMargin=RM,
                          topMargin=H - BODY_TOP, bottomMargin=BODY_BOT)
    fr  = Frame(LM, BODY_BOT, CW, BODY_H, id='b',
                leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0)
    doc.addPageTemplates([PageTemplate(id='m', frames=[fr], onPage=_page_cb(n))])
    doc.build(story)
    return buf.getvalue()


# ── Shared flowable helpers ─────────────────────────────────────
def _sec(s, txt):
    return [Paragraph(txt, s['h3']),
            HRFlowable(width=CW, thickness=0.6, color=BLUE, spaceAfter=3)]


def _addr(inv):
    return '\n'.join(filter(None, [
        inv.get('address_line1', ''),
        inv.get('address_line2', ''),
        ' '.join(filter(None, [inv.get('postcode', ''), inv.get('city', '')])),
        inv.get('state', ''),
    ]))


def _letter(s, name, addr, title, issued,
            stmt_type='', stmt_period='', page='1 of 1'):
    """
    Official letter block:
      Line 1: [empty left] | TITLE (right-aligned, large)
      Line 2: NAME + ADDRESS (left) | meta table (right) — fully polarised
    """
    # ── Title row: occupies full width, right-aligned ──────────
    title_p = Paragraph(f'<b>{title}</b>',
        ParagraphStyle('_tt', fontName='Helvetica-Bold', fontSize=13,
                       textColor=G1, alignment=TA_RIGHT, leading=16))
    title_row = Table([[title_p]], colWidths=[CW])
    title_row.setStyle(TableStyle([
        ('LEFTPADDING',(0,0),(-1,-1),0),('RIGHTPADDING',(0,0),(-1,-1),0),
        ('TOPPADDING',(0,0),(-1,-1),0),('BOTTOMPADDING',(0,0),(-1,-1),3),
    ]))

    # ── Meta table (right column) ──────────────────────────────
    meta = [('Page No.',    f': {page}'),
            ('Issued Date', f': {issued}')]
    if stmt_type:   meta.append(('Statement Type',   f': {stmt_type}'))
    if stmt_period: meta.append(('Statement Period', f': {stmt_period}'))
    meta += [('Email Address', ': nzy.invest@gmail.com'),
             ('Telephone No.', ': (+60)11 - 1121 8085')]

    meta_t = Table(
        [[Paragraph(k, s['small']), Paragraph(v, s['small'])] for k, v in meta],
        colWidths=[36*mm, 70*mm])
    meta_t.setStyle(TableStyle([
        ('TOPPADDING',(0,0),(-1,-1),1.5),
        ('BOTTOMPADDING',(0,0),(-1,-1),1.5),
        ('LEFTPADDING',(0,0),(-1,-1),0),
        ('RIGHTPADDING',(0,0),(-1,-1),0),
    ]))

    # ── Address table (left column) ────────────────────────────
    addr_rows = [[Paragraph(f'<b>{name.upper()}</b>',
        ParagraphStyle('_an', fontName='Helvetica-Bold', fontSize=9,
                       textColor=G1, spaceAfter=1))]]
    for ln in (addr or '').split('\n'):
        if ln.strip():
            addr_rows.append([Paragraph(ln.strip(), s['small'])])
    addr_t = Table(addr_rows, colWidths=[62*mm])
    addr_t.setStyle(TableStyle([
        ('TOPPADDING',(0,0),(-1,-1),1.5),
        ('BOTTOMPADDING',(0,0),(-1,-1),1.5),
        ('LEFTPADDING',(0,0),(-1,-1),0),
        ('RIGHTPADDING',(0,0),(-1,-1),0),
    ]))

    # ── Two-column body row ─────────────────────────────────────
    # Left: name+address, Right: meta — separator spacer in between
    body_row = Table([[addr_t, meta_t]],
                     colWidths=[62*mm, CW - 62*mm])
    body_row.setStyle(TableStyle([
        ('VALIGN',(0,0),(-1,-1),'TOP'),
        ('LEFTPADDING',(0,0),(-1,-1),0),
        ('RIGHTPADDING',(0,0),(-1,-1),0),
        ('TOPPADDING',(0,0),(-1,-1),0),
        ('BOTTOMPADDING',(0,0),(-1,-1),0),
    ]))

    return [
        title_row,
        Spacer(1, 5*mm),
        body_row,
        Spacer(1, 5*mm),
        HRFlowable(width=CW, thickness=0.4, color=G5, spaceAfter=4),
    ]


def _inv_grid(s, fields):
    """4-column investor info grid."""
    rows = [[
        Paragraph(r[0], s['small']),
        Paragraph(str(r[1]) if r[1] else '—', s['bold']),
        Paragraph(r[2], s['small']),
        Paragraph(str(r[3]) if r[3] else '—', s['bold']),
    ] for r in fields]
    w1 = 32*mm; w2 = CW/2 - w1; w3 = 32*mm; w4 = CW/2 - w3
    t = Table(rows, colWidths=[w1, w2, w3, w4])
    t.setStyle(TableStyle([
        ('GRID',(0,0),(-1,-1),0.4,G5),
        ('ROWBACKGROUNDS',(0,0),(-1,-1),[WHITE,G4]),
        ('TOPPADDING',(0,0),(-1,-1),5),
        ('BOTTOMPADDING',(0,0),(-1,-1),5),
        ('LEFTPADDING',(0,0),(-1,-1),6),
        ('FONTSIZE',(0,0),(-1,-1),8),
        ('TEXTCOLOR',(0,0),(0,-1),G2),
        ('TEXTCOLOR',(2,0),(2,-1),G2),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
    ]))
    return t


def _inv_block(s, inv):
    return [
        *_sec(s, "Investor's Information"),
        _inv_grid(s, [
            ["Account Type",    inv.get('account_type', 'Nominee Account'),
             "Account ID",      inv.get('account_id', '—')],
            ["Registered Name", inv.get('name', '—'),
             "Settlement Type", "Banking"],
            ["Phone No.",       inv.get('phone', '—'),
             "Bank Name",       inv.get('bank_name', '—')],
            ["Email Address",   inv.get('email', '—'),
             "Bank Account No.", inv.get('bank_account_no', '—')],
            ["Nominee Name",    "—",
             "Total Days Held", f"{inv.get('days_held', 0)} days"],
        ]),
        Spacer(1, 5*mm),
    ]


def _dtbl(s, headers, rows, col_w, total_row=None):
    def _c(v):
        if isinstance(v, Paragraph): return v
        return Paragraph(str(v) if v is not None else '—',
            ParagraphStyle('_c', fontName='Helvetica', fontSize=8,
                           textColor=G1, leading=11))
    hrow = [Paragraph(h, ParagraphStyle('_h', fontName='Helvetica-Bold',
                fontSize=8, textColor=WHITE, alignment=TA_CENTER)) for h in headers]
    td   = [hrow] + [[_c(c) for c in r] for r in rows]
    if total_row:
        td.append([(_c(c) if not isinstance(c, Paragraph) else c) for c in total_row])
    t = Table(td, colWidths=col_w, repeatRows=1)
    sty = [
        ('BACKGROUND',(0,0),(-1,0), BLUE),
        ('ROWBACKGROUNDS',(0,1),(-1,-2 if total_row else -1),[WHITE,G4]),
        ('GRID',(0,0),(-1,-1),0.4,G5),
        ('TOPPADDING',(0,0),(-1,-1),4),
        ('BOTTOMPADDING',(0,0),(-1,-1),4),
        ('LEFTPADDING',(0,0),(-1,-1),5),
        ('RIGHTPADDING',(0,0),(-1,-1),5),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
    ]
    if total_row:
        sty += [('BACKGROUND',(0,-1),(-1,-1),BLU_LT),
                ('LINEABOVE',(0,-1),(-1,-1),1,BLUE)]
    t.setStyle(TableStyle(sty))
    return t


# ── Charts ──────────────────────────────────────────────────────
def _nta_chart(dates, ntas, width=CW, height=58*mm):
    if not ntas: return None
    lo, hi = min(ntas), max(ntas)
    pad    = max((hi - lo) * 0.15, 0.05)
    y_min  = round((lo - pad) / 0.05) * 0.05
    y_max  = round((hi + pad) / 0.05 + 1) * 0.05

    fig, ax = plt.subplots(figsize=(width/mm/3.78, height/mm/3.78), dpi=150)
    x = list(range(len(dates)))
    ax.plot(x, ntas, color='#1565C0', linewidth=1.4, zorder=3)
    ax.fill_between(x, ntas, y_min, alpha=0.08, color='#1565C0')
    step = max(1, len(dates)//10)
    ax.set_xticks(x[::step])
    ax.set_xticklabels([dates[i] for i in x[::step]], fontsize=5.5, rotation=30, ha='right')
    ax.set_ylim(y_min, y_max)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter('%.3f'))
    ax.tick_params(axis='y', labelsize=5.5)
    ax.grid(axis='y', alpha=0.25, linewidth=0.4, linestyle='--')
    for sp in ['top','right']:    ax.spines[sp].set_visible(False)
    for sp in ['left','bottom']:  ax.spines[sp].set_color('#E5E7EB')
    # Annotate first + last NTA
    for idx, lbl in [(0, f'{ntas[0]:.4f}'), (len(ntas)-1, f'{ntas[-1]:.4f}')]:
        ax.annotate(lbl, xy=(idx, ntas[idx]),
            xytext=(6 if idx else -6, 5), textcoords='offset points',
            fontsize=5.5, color='#1565C0', fontweight='bold',
            ha='left' if idx else 'right')
    plt.tight_layout(pad=0.3)
    buf = io.BytesIO()
    fig.savefig(buf, format='png', transparent=True, bbox_inches='tight', dpi=150)
    plt.close(fig); buf.seek(0)
    return Image(buf, width=width, height=height)


def _donut(labels, values, size=(55*mm, 55*mm)):
    if not values or sum(values) == 0: return None
    fig, ax = plt.subplots(figsize=(2.2, 2.2), dpi=150)
    _, _, autotexts = ax.pie(
        values, autopct=lambda p: f'{p:.1f}%' if p > 4 else '',
        pctdistance=0.75,
        colors=[CC[i % len(CC)] for i in range(len(values))],
        startangle=90,
        wedgeprops=dict(width=0.52, edgecolor='white', linewidth=1.2))
    for at in autotexts:
        at.set_fontsize(5.5); at.set_color('white'); at.set_fontweight('bold')
    ax.set_aspect('equal')
    plt.tight_layout(pad=0.1)
    buf = io.BytesIO()
    fig.savefig(buf, format='png', transparent=True, bbox_inches='tight', dpi=150)
    plt.close(fig); buf.seek(0)
    return Image(buf, width=size[0], height=size[1])


# ══════════════════════════════════════════════════════════════
# 1.  FACTSHEET
# ══════════════════════════════════════════════════════════════
def generate_factsheet(fund_data, holdings, performance, distributions,
                       nta_history, sector_data, manager_comment=''):
    s     = S()
    story = []
    today = date.today()

    # Title block (right-aligned, no letter header)
    story.append(Paragraph('<b>ZY Family Vision Portfolio</b>',
        ParagraphStyle('ft', fontName='Helvetica-Bold', fontSize=15,
                       textColor=G1, alignment=TA_RIGHT)))
    story.append(Paragraph(today.strftime('%B %Y'),
        ParagraphStyle('fp', fontName='Helvetica', fontSize=10,
                       textColor=G2, alignment=TA_RIGHT, spaceAfter=3)))
    story.append(HRFlowable(width=CW, thickness=0.4, color=G5, spaceAfter=4))
    story.append(Paragraph(
        'The portfolio aims to provide our investors with capital appreciation higher than the '
        'prevailing fixed-deposit rate by investing in a high-growth portfolio of stocks and '
        'fixed income instruments.', s['body']))
    story.append(Spacer(1, 5*mm))

    # ── Two-column: Fund Details (left) | Sector Donut (right) ─
    nta_v = fund_data.get('current_nta', 0)
    fd_rows = [
        ('Manager',               'Mr. Ng Zhi Yao'),
        ('Fund Category',         'Equity Fund'),
        ('Launch Date',           '13 December 2021'),
        ('Unit Net Asset Value',  fm(nta_v, 4)),
        ('Fund Size',             fm(fund_data.get('aum', 0))),
        ('Units in Circulation',  fn(fund_data.get('total_units', 0), 2) + ' units'),
        ('Financial Year End',    '30 November'),
        ('Min. Initial Investment','RM 10,000.00'),
        ('Min. Additional',       'RM 1,000.00'),
        ('Benchmark',             'FBMKLCI, Tech Index'),
        ('Annual Mgmt Fee',       '0.10% p.a. of NAV'),
        ('Performance Fee',       '10.0% p.a. of excess return'),
        ('Distribution Policy',   'At least 80% of gross income'),
    ]
    fd_t = Table(
        [[Paragraph(k, s['small']), Paragraph(v, s['bold'])] for k, v in fd_rows],
        colWidths=[40*mm, 55*mm])
    fd_t.setStyle(TableStyle([
        ('ROWBACKGROUNDS',(0,0),(-1,-1),[WHITE,G4]),
        ('GRID',(0,0),(-1,-1),0.4,G5),
        ('TOPPADDING',(0,0),(-1,-1),3.5),
        ('BOTTOMPADDING',(0,0),(-1,-1),3.5),
        ('LEFTPADDING',(0,0),(-1,-1),5),
        ('FONTSIZE',(0,0),(-1,-1),7.5),
        ('TEXTCOLOR',(0,0),(0,-1),G2),
    ]))

    # Sector donut block (right)
    right_story = []
    right_story.append(Paragraph('Sector Allocation*', s['h3']))
    right_story.append(HRFlowable(width='100%', thickness=0.6, color=BLUE, spaceAfter=3))
    if sector_data:
        lbls = [r.get('asset_class') or r.get('sector', '') for r in sector_data[:6]]
        vals = [max(float(r.get('weight_pct', 0)), 0.01) for r in sector_data[:6]]
        pie  = _donut(lbls, vals, size=(55*mm, 55*mm))
        if pie:
            legend = [[
                Paragraph(f'<font color="{CC[i%8]}">■</font>  {lbls[i]}', s['tiny']),
                Paragraph(f'{vals[i]:.1f}%', s['tiny'])
            ] for i in range(len(lbls))]
            leg_t = Table(legend, colWidths=[46*mm, 12*mm])
            leg_t.setStyle(TableStyle([
                ('TOPPADDING',(0,0),(-1,-1),2),
                ('BOTTOMPADDING',(0,0),(-1,-1),2),
                ('LEFTPADDING',(0,0),(-1,-1),2),
            ]))
            right_story.append(Table([[pie, leg_t]], colWidths=[57*mm, 60*mm]))

    # Left column: title + table
    L_W = 100*mm
    R_W = CW - L_W - 3*mm   # 3mm gap

    left_cells  = [Paragraph('Fund Details', s['h3']),
                   HRFlowable(width='100%', thickness=0.6, color=BLUE, spaceAfter=3),
                   fd_t]
    right_cells = right_story

    # Each column wrapped as a list inside a single-cell Table
    left_t  = Table([[c] for c in left_cells],  colWidths=[L_W])
    right_t = Table([[c] for c in right_cells], colWidths=[R_W])
    for t in [left_t, right_t]:
        t.setStyle(TableStyle([
            ('LEFTPADDING',(0,0),(-1,-1),0),
            ('RIGHTPADDING',(0,0),(-1,-1),0),
            ('TOPPADDING',(0,0),(-1,-1),0),
            ('BOTTOMPADDING',(0,0),(-1,-1),1),
        ]))

    two_col = Table([[left_t, Spacer(3*mm,1), right_t]], colWidths=[L_W, 3*mm, R_W])
    two_col.setStyle(TableStyle([
        ('VALIGN',(0,0),(-1,-1),'TOP'),
        ('LEFTPADDING',(0,0),(-1,-1),0),
        ('RIGHTPADDING',(0,0),(-1,-1),0),
        ('TOPPADDING',(0,0),(-1,-1),0),
        ('BOTTOMPADDING',(0,0),(-1,-1),0),
    ]))
    story.append(two_col)
    story.append(Spacer(1, 4*mm))

    # Investment strategy
    story += _sec(s, 'Investment Strategy')
    story.append(Paragraph(
        'Equities: min 60% / max 98%.  '
        'Derivatives: max 5%.  Fixed-income: min 2% / max 40%.', s['body']))
    story.append(Spacer(1, 3*mm))

    # NTA performance chart
    if nta_history:
        story += _sec(s, 'Portfolio Performance Analysis')
        dates, ntas = [], []
        for r in nta_history:
            try:
                dt = datetime.fromisoformat(str(r.get('date', '')))
                dates.append(dt.strftime('%b-%y'))
                ntas.append(float(r.get('nta', 1)))
            except: pass
        if ntas:
            ch = _nta_chart(dates, ntas, width=CW, height=58*mm)
            if ch: story.append(ch); story.append(Spacer(1, 2*mm))
        perf = performance.get('period_returns', {})
        story.append(_dtbl(s,
            headers=['', '1M', '3M', '6M', '1Y', '3Y', 'All'],
            rows=[['Portfolio'] + [fp(perf.get(k)) for k in ['1M','3M','6M','1Y','3Y']]
                  + [fp(fund_data.get('total_return_pct', 0))]],
            col_w=[28*mm, 23*mm, 23*mm, 23*mm, 23*mm, 23*mm, 23*mm]))
        story.append(Spacer(1, 3*mm))

    # Distribution history
    if distributions:
        story += _sec(s, 'Distribution History')
        story.append(_dtbl(s,
            headers=['Year', 'Description', 'DPS (sen)', 'Payout Ratio'],
            rows=[[d.get('financial_year','—'), d.get('title','—'),
                   fn(d.get('dps_sen'),2)+' sen',
                   f"{float(d['payout_ratio']):.1f}%" if d.get('payout_ratio') else '—']
                  for d in distributions],
            col_w=[18*mm, 101*mm, 27*mm, 25*mm]))
        story.append(Spacer(1, 3*mm))

    # Manager's Comments
    story += _sec(s, "Manager's Comments")
    story.append(Paragraph(manager_comment or
        'Our portfolio maintains its current strategic positions. We continue to monitor '
        'market developments and macroeconomic conditions closely.', s['body']))
    story.append(Spacer(1, 3*mm))

    # Largest holdings
    if holdings:
        story += _sec(s, 'Largest Holdings*')
        story.append(_dtbl(s,
            headers=['Asset Name', 'Percentage'],
            rows=[[h.get('instrument','—'), f"{float(h.get('weight_pct',0)):.2f}%"]
                  for h in holdings[:10]],
            col_w=[143*mm, 28*mm]))
        story.append(Paragraph('* As percentage of Net Asset Value (NAV) of the fund',
                                s['tiny']))
        story.append(Spacer(1, 2*mm))

    # Factsheet disclaimer (inline, not in footer zone)
    story.append(HRFlowable(width=CW, thickness=0.5, color=G5))
    story.append(Spacer(1, 2*mm))
    story.append(Paragraph('<b>Disclaimer</b>',
        ParagraphStyle('_d', fontName='Helvetica-Bold', fontSize=8, textColor=G1)))
    story.append(Paragraph(
        'Investment involves significant risk, including the potential loss of principal. '
        'Past performance is not indicative of future results. This update is intended strictly '
        'for family members only and does not constitute a financial prospectus.',
        s['notice']))

    return _build(story)


# ══════════════════════════════════════════════════════════════
# 2.  SUBSCRIPTION STATEMENT  — one record per PDF
# ══════════════════════════════════════════════════════════════
def generate_subscription(investor, cashflow_record):
    s = S(); story = []
    today    = date.today()
    inv_date = cashflow_record.get('date', '')
    amt      = float(cashflow_record.get('amount', 0))
    units    = float(cashflow_record.get('units', 0))
    nta_d    = cashflow_record.get('nta_at_date')

    story += _letter(s, investor.get('name',''), _addr(investor),
        'FUND SUBSCRIPTION STATEMENT', today.strftime('%d-%m-%Y'),
        stmt_type='Daily Statement',
        stmt_period=fd(inv_date) if inv_date else today.strftime('%d/%m/%Y'))
    story += _inv_block(s, investor)
    story += _sec(s, 'Principal Transaction')
    story.append(_dtbl(s,
        headers=['Date', 'Description', 'Investment Value',
                 'Subscription Price', 'Unit Balanced', 'Average Cost'],
        rows=[
            [fd(inv_date), 'Opening', '—', '—', '—', '—'],
            [fd(inv_date), 'Fund Subscription',
             fm(abs(amt)), fn(nta_d, 4) if nta_d else '—',
             fn(units, 4), fn(nta_d, 4) if nta_d else '—'],
            [fd(inv_date), 'Closing',
             fm(abs(amt)), '—', fn(units, 4), fn(nta_d, 4) if nta_d else '—'],
        ],
        col_w=[24*mm, 40*mm, 30*mm, 28*mm, 26*mm, 23*mm]))
    return _build(story)


# ══════════════════════════════════════════════════════════════
# 3.  REDEMPTION STATEMENT  — one record per PDF
# ══════════════════════════════════════════════════════════════
def generate_redemption(investor, cashflow_record):
    s = S(); story = []
    today    = date.today()
    inv_date = cashflow_record.get('date', '')
    amt      = float(cashflow_record.get('amount', 0))
    units    = abs(float(cashflow_record.get('units', 0)))
    nta_d    = cashflow_record.get('nta_at_date')

    story += _letter(s, investor.get('name',''), _addr(investor),
        'FUND REDEMPTION STATEMENT', today.strftime('%d-%m-%Y'),
        stmt_type='Daily Statement',
        stmt_period=fd(inv_date) if inv_date else today.strftime('%d/%m/%Y'))
    story += _inv_block(s, investor)
    story += _sec(s, 'Redemption Transaction')
    story.append(_dtbl(s,
        headers=['Date', 'Description', 'Redemption Value',
                 'Redemption Price', 'Units Redeemed', 'Average Cost'],
        rows=[
            [fd(inv_date), 'Opening', '—', '—', '—', '—'],
            [fd(inv_date), 'Fund Redemption',
             fm(abs(amt)), fn(nta_d, 4) if nta_d else '—',
             fn(units, 4), fn(nta_d, 4) if nta_d else '—'],
            [fd(inv_date), 'Closing',
             fm(abs(amt)), '—', fn(units, 4), '—'],
        ],
        col_w=[24*mm, 40*mm, 30*mm, 28*mm, 26*mm, 23*mm]))
    return _build(story)


# ══════════════════════════════════════════════════════════════
# 4.  DIVIDEND PAYMENT STATEMENT  — one distribution per investor per PDF
# ══════════════════════════════════════════════════════════════
def generate_dividend_statement(investor, dist_record, financial_year):
    s = S(); story = []
    today = date.today()
    dps   = float(dist_record.get('dps_sen', 0))
    units = float(dist_record.get('units_at_ex_date', 0))
    amt   = float(dist_record.get('amount', 0))
    eps   = dist_record.get('eps')
    dpr   = dist_record.get('payout_ratio')
    pmt   = dist_record.get('pmt_date') or dist_record.get('ex_date')
    title_lbl = dist_record.get('title', dist_record.get('dist_type', financial_year))

    story += _letter(s, investor.get('name',''), _addr(investor),
        'DIVIDEND PAYMENT STATEMENT', today.strftime('%d-%m-%Y'),
        stmt_type=financial_year, stmt_period=fd(pmt))
    story += [
        *_sec(s, "Investor's Information"),
        _inv_grid(s, [
            ["Investor's Name", investor.get('name','—'), "Settlement Type",   "Banking"],
            ["Phone Number",    investor.get('phone','—'),"Bank Name",         investor.get('bank_name','—')],
            ["Email Address",   investor.get('email','—'),"Bank Account No.",  investor.get('bank_account_no','—')],
        ]),
        Spacer(1, 5*mm),
    ]
    story += _sec(s, 'Dividend Details')
    story.append(_dtbl(s,
        headers=['Date', 'Description', 'Holding Units', 'EPS', 'DPR', 'DPS (sen)', 'Amount'],
        rows=[[
            fd(pmt), title_lbl, fn(units, 4),
            fn(eps, 2) if eps else '—',
            f"{float(dpr):.1f}%" if dpr else '—',
            fn(dps, 2),
            Paragraph(f'<b>{fm(amt)}</b>',
                ParagraphStyle('_am', fontName='Helvetica-Bold', fontSize=8,
                               textColor=BLUE, alignment=TA_RIGHT)),
        ]],
        col_w=[24*mm, 40*mm, 24*mm, 14*mm, 14*mm, 18*mm, 37*mm],
        total_row=['', 'Total', '', '', '', fn(dps, 2),
            Paragraph(f'<b>{fm(amt)}</b>',
                ParagraphStyle('_ta', fontName='Helvetica-Bold', fontSize=8,
                               textColor=BLUE, alignment=TA_RIGHT))]))
    story.append(Paragraph(
        'Notes: EPS = Earning Per Share  |  DPR = Dividend Payout Ratio  |  DPS = Dividend Per Share (sen)',
        s['tiny']))
    return _build(story)


# ══════════════════════════════════════════════════════════════
# 5.  INVESTMENT ACCOUNT STATEMENT  (annual, 2 pages)
# ══════════════════════════════════════════════════════════════
def generate_account_statement(investor, summary, cashflows, dist_history,
                                statement_period, financial_year):
    s = S(); story = []
    today = date.today()
    name  = investor.get('name', '')
    addr  = _addr(investor)

    # ── Page 1 ─────────────────────────────────────────────────
    story += _letter(s, name, addr, 'INVESTMENT ACCOUNT STATEMENT',
        today.strftime('%d-%m-%Y'), stmt_type='Annually',
        stmt_period=statement_period, page='1 of 2')
    story += _inv_block(s, investor)

    # Account summary
    nta_v  = float(summary.get('current_nta', 0))
    units_v = float(summary.get('units', 0))
    cost_v  = float(summary.get('total_costs', 0))
    mv_v    = float(summary.get('market_value', 0))
    unr_v   = float(summary.get('unrealized_pl', 0))
    rea_v   = float(summary.get('realized_pl', 0))
    div_v   = float(summary.get('dividends_received', 0))
    adj_v   = float(summary.get('adjustment', 0))
    tpl     = unr_v + rea_v + div_v + adj_v
    tpct    = (tpl / abs(cost_v) * 100) if cost_v else 0
    irr     = summary.get('irr')
    avg_c   = cost_v / units_v if units_v else 0

    def _pr(t, a=TA_LEFT):
        return Paragraph(str(t), ParagraphStyle('_pr', fontName='Helvetica',
            fontSize=8, textColor=G1, alignment=a, leading=11))
    def _bl(t):
        return Paragraph(f'<b>{t}</b>',
            ParagraphStyle('_bl', fontName='Helvetica-Bold', fontSize=8, textColor=G1))
    def _bcr(v, c):
        return Paragraph(f'<b>{v}</b>',
            ParagraphStyle('_bcr', fontName='Helvetica-Bold', fontSize=8,
                           textColor=c, alignment=TA_RIGHT))

    tc  = GREEN if tpl  >= 0 else RED
    tpc = GREEN if tpct >= 0 else RED
    ic  = GREEN if (irr and float(irr) >= 0) else RED

    hdr = [Paragraph(h, ParagraphStyle('_sh', fontName='Helvetica-Bold',
        fontSize=8, textColor=WHITE, alignment=TA_CENTER))
        for h in ['', 'Fields', 'Holding Units', 'Average Price', 'Total Value (RM)']]
    rows_data = [
        [_pr('(a)'), _pr('Latest Fund Price'),                     _pr(fn(units_v,4),TA_RIGHT), _pr(fn(nta_v,4),TA_RIGHT),  _pr(fm(mv_v),TA_RIGHT)],
        [_pr('(b)'), _pr('Subscription Cost'),                     _pr(fn(units_v,4),TA_RIGHT), _pr(fn(avg_c,4),TA_RIGHT),  _pr(f'({fm(cost_v)})',TA_RIGHT)],
        [_pr('(c)'), _pr('Unrealized P&L:  (a) + (b)'),           _pr('',TA_RIGHT),             _pr('',TA_RIGHT),            _pr(fm(unr_v),TA_RIGHT)],
        [_pr('(d)'), _pr('Realized Profit & Loss'),                _pr('',TA_RIGHT),             _pr('',TA_RIGHT),            _pr(fm(rea_v) if rea_v else '—',TA_RIGHT)],
        [_pr('(e)'), _pr('Dividend Received'),                     _pr('',TA_RIGHT),             _pr('',TA_RIGHT),            _pr(fm(div_v) if div_v else '—',TA_RIGHT)],
        [_pr('(f)'), _pr(f'Adjustment / Fund Switching {fm(abs(adj_v))}'),
                                                                   _pr('',TA_RIGHT),             _pr('',TA_RIGHT),
            _pr(f'({fm(abs(adj_v))})' if adj_v < 0 else (fm(adj_v) if adj_v else '—'),TA_RIGHT)],
    ]
    td = [hdr] + rows_data + [
        [_pr(''), _bl('Total Profit & Loss:  (c)+(d)+(e)+(f)'), _pr(''), _pr(''), _bcr(fm(tpl), tc)],
        [_pr(''), _bl('Total Performance %'),                   _pr(''), _pr(''), _bcr(fp(tpct), tpc)],
        [_pr(''), _bl('Annualised Performance* %'),             _pr(''), _pr(''), _bcr(fp(irr) if irr else '—', ic)],
    ]
    sum_t = Table(td, colWidths=[9*mm, 84*mm, 26*mm, 26*mm, 26*mm])
    sum_t.setStyle(TableStyle([
        ('BACKGROUND',(0,0),(-1,0), BLUE),
        ('ROWBACKGROUNDS',(0,1),(-1,-4),[WHITE,G4]),
        ('GRID',(0,0),(-1,-1),0.4,G5),
        ('TOPPADDING',(0,0),(-1,-1),4), ('BOTTOMPADDING',(0,0),(-1,-1),4),
        ('LEFTPADDING',(0,0),(-1,-1),5), ('RIGHTPADDING',(0,0),(-1,-1),5),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
        ('LINEABOVE',(0,-3),(-1,-3),1,BLUE),
        ('BACKGROUND',(0,-3),(-1,-1),BLU_LT),
    ]))
    story += _sec(s, 'Account Summary')
    story.append(sum_t)
    story.append(Paragraph(
        '* Powered by Internal Rate of Return (IRR) & Newton\'s method', s['tiny']))

    # ── Page 2 ─────────────────────────────────────────────────
    story.append(PageBreak())
    story += _letter(s, name, addr, 'INVESTMENT ACCOUNT STATEMENT',
        today.strftime('%d-%m-%Y'), stmt_type='Annually',
        stmt_period=statement_period, page='2 of 2')

    if cashflows:
        running = 0.0
        cf_rows = []
        for cf in cashflows:
            u   = float(cf.get('units', 0))
            amt = float(cf.get('amount', 0))
            running += u
            nta_d = cf.get('nta_at_date')
            cf_rows.append([
                fd(cf.get('date')),
                cf.get('description', '—'),
                fm(abs(amt)) if amt else '—',
                fm(nta_d) if nta_d else '—',
                fn(abs(u), 4) if u else '—',
                fn(running, 4) if running > 0 else '—',
            ])
        story += _sec(s, 'Principal Transaction')
        story.append(_dtbl(s,
            headers=['Date','Description','Cashflow','Avg. Cost (RM)','Units Issued','Units Balanced'],
            rows=cf_rows, col_w=[22*mm, 48*mm, 32*mm, 24*mm, 22*mm, 23*mm]))

    if dist_history:
        story += _sec(s, 'Distribution Transaction')
        story.append(_dtbl(s,
            headers=['Date','Description','DPS (sen)','Holding Units','Amount','Balanced (RM)'],
            rows=[[fd(d.get('pmt_date') or d.get('ex_date')), d.get('title','—'),
                   fn(d.get('dps_sen'),2),
                   fn(d.get('units_at_ex_date'),4) if d.get('units_at_ex_date') else '—',
                   fm(d.get('amount')) if d.get('amount') else '—',
                   fm(d.get('amount')) if d.get('amount') else '—']
                  for d in dist_history],
            col_w=[22*mm, 48*mm, 20*mm, 28*mm, 28*mm, 25*mm]))

    return _build(story)
