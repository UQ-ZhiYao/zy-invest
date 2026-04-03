"""
ZY-Invest PDF Statement Generator  v7.0
Fixes vs v6:
  1. TITLE drawn on CANVAS top-right (same height as logo, above blue line) — not in body
  2. Name+Address left-aligned, address lines below name, NO truncation
  3. Meta table flush-right using canvas drawRightString — not a flowable Table
  4. All tables exactly CW wide (verified maths)
  5. Disclaimer uses pdfmetrics.stringWidth for correct word-wrap — no orphan words
"""
import io, os, copy
from datetime import date, datetime

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.platypus import (
    BaseDocTemplate, Frame, PageTemplate, PageBreak,
    Paragraph, Spacer, Table, TableStyle, HRFlowable, Image
)
from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT, TA_JUSTIFY

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

W, H   = A4          # 595.28 x 841.89 pt
LM = RM = 12 * mm   # left/right margins  →  CW ≈ 527 pt ≈ 186 mm

# ── Canvas zones (all in pts from page bottom) ─────────────────
HDR_H    = 32 * mm   # header zone height
HDR_LINE = H - HDR_H # blue line y-position

# Logo: drawn from (LM, HDR_LINE+4mm) upward, 22mm tall
LOGO_Y  = HDR_LINE + 4 * mm
LOGO_H  = 22 * mm
LOGO_W  = 28 * mm

# Title: drawn on canvas, right-aligned, vertically centred in header
TITLE_Y  = HDR_LINE + HDR_H / 2 - 5 * mm   # ~middle of header zone

# Footer
FTR_LINE = 17 * mm
FTR_TEXT = 10 * mm

# Disclaimer zone — computed exactly at draw time (see _draw_disclaimer)
# Reserve 22mm above footer for disclaimer: separator + title + 3 notices
DISC_H   = 22 * mm
DISC_TOP = FTR_LINE + DISC_H

# Body frame — stops just above disclaimer zone
BODY_BOT = DISC_TOP + 1 * mm
BODY_TOP = HDR_LINE - 4 * mm
BODY_H   = BODY_TOP - BODY_BOT
CW       = W - LM - RM          # ≈ 527 pt

LOGO = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logo.png')
CC   = ['#1565C0','#2E7D32','#E65100','#7B1FA2','#00838F','#BF360C','#4527A0','#558B2F']


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
        'h3':    p('h3',   fontName='Helvetica-Bold', fontSize=9,    textColor=BLUE, spaceBefore=4, spaceAfter=2),
        'body':  p('body', fontName='Helvetica',      fontSize=8.5,  textColor=G1,   leading=13),
        'small': p('sm',   fontName='Helvetica',      fontSize=7.5,  textColor=G2,   leading=11),
        'tiny':  p('tiny', fontName='Helvetica',      fontSize=7,    textColor=G3,   leading=10),
        'bold':  p('b',    fontName='Helvetica-Bold', fontSize=8.5,  textColor=G1),
        'notice':p('n',    fontName='Helvetica',      fontSize=7.5,  textColor=G2,   leading=11),
    }


# ── Canvas text helpers ─────────────────────────────────────────
def _canvas_wrap(canvas, text, x, y, font, size, max_w, line_h):
    """Draw wrapped text on canvas using exact stringWidth measurements."""
    words = text.split()
    line  = ''
    for w in words:
        test = (line + ' ' + w).strip()
        if pdfmetrics.stringWidth(test, font, size) > max_w and line:
            canvas.drawString(x, y, line)
            y -= line_h
            line = w
        else:
            line = test
    if line:
        canvas.drawString(x, y, line)
        y -= line_h
    return y  # return new y after last line


# ── Per-page canvas callback ────────────────────────────────────
def _page_cb(n_pages, title='', subtitle='', name='', addr='', meta_rows=None):
    """
    Canvas draws on EVERY page:
      - Logo (top-left, above blue line)
      - Statement title (top-right, above blue line, same zone as logo)
      - Blue HR
      - Footer HR + text + page number
    On LAST page additionally draws:
      - Name + Address (left, below blue line, in body-top area)
      - Meta table (right, below blue line, same zone as name/address)
      - Thin separator HR below name/address block
      - Disclaimer + Important Notices above footer
    """
    meta_rows = meta_rows or []

    def _draw(canvas, doc):
        canvas.saveState()
        pg = doc.page

        # ════════════════════════════════════════════════════
        # HEADER — drawn on every page above blue line
        # ════════════════════════════════════════════════════

        # Logo (top-left)
        if os.path.exists(LOGO):
            try:
                canvas.drawImage(LOGO, LM, LOGO_Y,
                                 width=LOGO_W, height=LOGO_H,
                                 preserveAspectRatio=True, anchor='sw', mask='auto')
            except Exception:
                canvas.setFont('Helvetica-Bold', 11)
                canvas.setFillColor(BLUE)
                canvas.drawString(LM, LOGO_Y + LOGO_H / 2, 'ZY-Invest')

        # Title — top-right, same zone as logo, EVERY page
        if title:
            canvas.setFont('Helvetica-Bold', 12)
            canvas.setFillColor(G1)
            canvas.drawRightString(W - RM, TITLE_Y + (3*mm if subtitle else 0), title)
            if subtitle:
                canvas.setFont('Helvetica', 9)
                canvas.setFillColor(G2)
                canvas.drawRightString(W - RM, TITLE_Y - 3*mm, subtitle)

        # Blue HR
        canvas.setStrokeColor(BLUE)
        canvas.setLineWidth(1.5)
        canvas.line(LM, HDR_LINE, W - RM, HDR_LINE)

        # ════════════════════════════════════════════════════
        # LETTER BLOCK — only on page 1, drawn at body TOP
        # (below blue line, inside body frame top area)
        # ════════════════════════════════════════════════════
        if name:
            # Draw letter block on every page (each page gets its own meta_rows via closure)
            LB_Y = BODY_TOP - 2*mm   # start 2mm below top of body

            # ── Left: Name + Address ─────────────────────────────
            canvas.setFont('Helvetica-Bold', 9)
            canvas.setFillColor(G1)
            canvas.drawString(LM, LB_Y, name.upper())
            LB_Y -= 4*mm

            canvas.setFont('Helvetica', 7.5)
            canvas.setFillColor(G2)
            for line in (addr or '').split('\n'):
                if line.strip():
                    canvas.drawString(LM, LB_Y, line.strip())
                    LB_Y -= 3.5*mm

            # ── Right: Meta rows ─────────────────────────────────
            meta_y   = BODY_TOP - 2*mm
            label_x  = W - RM - 64*mm   # label: 64mm from right edge
            colon_x  = label_x + 32*mm  # colon after label
            value_x  = colon_x + 3*mm   # value: 29mm space to right margin

            canvas.setFont('Helvetica', 7.5)
            for label, value in meta_rows:
                canvas.setFillColor(G2)
                canvas.drawString(label_x, meta_y, label)
                canvas.drawString(colon_x, meta_y, ':')
                canvas.setFillColor(G1)
                canvas.drawString(value_x, meta_y, str(value))
                meta_y -= 3.8*mm

            # ── Separator HR below letter block ──────────────────
            # Use the lower of the two columns as the separator y
            sep_y = min(LB_Y, meta_y) - 2*mm
            canvas.setStrokeColor(G5)
            canvas.setLineWidth(0.4)
            canvas.line(LM, sep_y, W - RM, sep_y)

            # Store sep_y so body content knows where to start
            # We do this by setting a doc custom attr (read by _count_pages too)
            doc._letter_sep_y = sep_y

        # ════════════════════════════════════════════════════
        # FOOTER — drawn on every page
        # ════════════════════════════════════════════════════
        canvas.setStrokeColor(G5)
        canvas.setLineWidth(0.5)
        canvas.line(LM, FTR_LINE, W - RM, FTR_LINE)

        canvas.setFont('Helvetica', 6.5)
        canvas.setFillColor(G3)
        canvas.drawString(LM, FTR_TEXT,
            'Head Office: None  |  Line: (60)11-1121 8085  |  '
            'Email: nzy.invest@gmail.com  |  Website: zy-invest.com')
        canvas.drawRightString(W - RM, FTR_TEXT, f'Page {pg}')

        # ════════════════════════════════════════════════════
        # DISCLAIMER — last page only, anchored FROM footer UP
        # ════════════════════════════════════════════════════
        if pg == n_pages:
            notices = [
                '1. Confidentiality: This statement contains personal data intended solely for '
                'the recipient. Please do not share this document with any third parties.',
                '2. Discrepancies: Please review all figures carefully. Any discrepancies must '
                'be reported to us immediately; failure to do so may result in the recipient '
                'bearing any losses.',
                '3. Digital Statements: Effective 1st January 2026, all future portfolio '
                'statements will be provided exclusively via WhatsApp.',
            ]
            lh     = 2.9 * mm
            gap    = 0.8 * mm
            fnt    = 'Helvetica'
            fnt_sz = 7

            # Measure exact total height of notices block
            def _line_count(text):
                words = text.split(); line = ''; n = 0
                for w in words:
                    test = (line + ' ' + w).strip()
                    if pdfmetrics.stringWidth(test, fnt, fnt_sz) > CW and line:
                        n += 1; line = w
                    else: line = test
                return n + (1 if line else 0)

            notices_h = sum(_line_count(n) * lh for n in notices)
            notices_h += gap * (len(notices) - 1)

            # Disclaimer block height:
            #  2mm above footer + 1.5mm separator gap + title 3mm + 1.5mm to notices
            TITLE_SZ  = 7.5
            above_ftr = 2 * mm      # gap above footer line
            sep_gap   = 2.5 * mm   # gap: footer→separator
            sep_title = 2.5 * mm   # gap: separator→title
            title_h   = 3 * mm     # title text height
            title_n   = 2 * mm     # gap: title→first notice

            # Total = notices_h + title_n + title_h + sep_title + sep_gap + above_ftr
            total_h = notices_h + title_n + title_h + sep_title + sep_gap + above_ftr

            # Starting Y positions (bottom-up anchored)
            base_y  = FTR_LINE + above_ftr          # just above footer line
            sep_y   = base_y + notices_h + title_n + title_h + sep_title
            title_y = sep_y - sep_title - title_h + 1*mm
            first_y = title_y - title_n - lh        # top of first notice line

            # Draw separator
            canvas.setStrokeColor(G5)
            canvas.setLineWidth(0.4)
            canvas.line(LM, sep_y, W - RM, sep_y)

            # Draw title
            canvas.setFont('Helvetica-Bold', TITLE_SZ)
            canvas.setFillColor(G1)
            canvas.drawString(LM, title_y, 'IMPORTANT NOTICES')

            # Draw notices top-down from first_y
            canvas.setFont(fnt, fnt_sz)
            canvas.setFillColor(G2)
            ny = first_y
            for i, notice in enumerate(notices):
                ny = _canvas_wrap(canvas, notice, LM, ny, fnt, fnt_sz, CW, lh)
                if i < len(notices) - 1:
                    ny -= gap

        canvas.restoreState()
    return _draw


# ── Two-pass build ──────────────────────────────────────────────
def _count_pages(story, extra_top=0):
    """Count pages needed. extra_top reserves space at body top for letter block."""
    class _C(BaseDocTemplate):
        def __init__(self):
            super().__init__(io.BytesIO(), pagesize=A4,
                             leftMargin=LM, rightMargin=RM,
                             topMargin=H - BODY_TOP + extra_top,
                             bottomMargin=BODY_BOT)
            fr = Frame(LM, BODY_BOT, CW, BODY_H - extra_top, id='b',
                       leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0)
            self.addPageTemplates([PageTemplate(id='m', frames=[fr])])
            self._n = 0
        def handle_pageEnd(self):
            self._n += 1; super().handle_pageEnd()
    c = _C(); c.build(copy.deepcopy(story)); return max(c._n, 1)


def _build(story, title='', subtitle='', name='', addr='', meta_rows=None, extra_top=0):
    """Build PDF with canvas-drawn header letter block."""
    n   = _count_pages(story, extra_top)
    buf = io.BytesIO()
    doc = BaseDocTemplate(buf, pagesize=A4,
                          leftMargin=LM, rightMargin=RM,
                          topMargin=H - BODY_TOP + extra_top,
                          bottomMargin=BODY_BOT)
    fr  = Frame(LM, BODY_BOT, CW, BODY_H - extra_top, id='b',
                leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0)
    cb  = _page_cb(n, title=title, subtitle=subtitle, name=name, addr=addr, meta_rows=meta_rows)
    doc.addPageTemplates([PageTemplate(id='m', frames=[fr], onPage=cb)])
    doc.build(story)
    return buf.getvalue()


# ── Letter meta builder ─────────────────────────────────────────
def _meta(page, issued, stmt_type='', stmt_period=''):
    rows = [('Page No.',    page), ('Issued Date', issued)]
    if stmt_type:   rows.append(('Statement Type',   stmt_type))
    if stmt_period: rows.append(('Statement Period', stmt_period))
    rows += [('Email Address', 'nzy.invest@gmail.com'),
             ('Telephone No.', '(+60)11 - 1121 8085')]
    return rows


def _addr(inv):
    """Build address lines from DB fields. Filters out None and empty strings."""
    def s(k): return (inv.get(k) or '').strip()
    postcode_city = ' '.join(x for x in [s('postcode'), s('city')] if x)
    state_country = ' '.join(x for x in [s('state'), s('country')] if x)
    return '\n'.join(x for x in [
        s('address_line1'),
        s('address_line2'),
        postcode_city,
        state_country,
    ] if x)


# ── Letter block height estimator ──────────────────────────────
def _letter_height(name, addr, meta_rows):
    """Estimate height of letter block to reserve in frame."""
    addr_lines = [l for l in (addr or '').split('\n') if l.strip()]
    name_h    = 4*mm
    addr_h    = len(addr_lines) * 3.5*mm
    meta_h    = len(meta_rows)  * 3.8*mm
    content_h = max(name_h + addr_h, meta_h)
    return content_h + 4*mm  # +4mm for separator and gaps (tightened)


# ── Shared table helpers ────────────────────────────────────────
def _sec(s, txt):
    return [Spacer(1, 6*mm),
            Paragraph(txt, s['h3']),
            HRFlowable(width=CW, thickness=0.6, color=BLUE, spaceAfter=3)]


def _inv_grid(s, fields):
    """4-column investor info grid — exactly CW wide."""
    # col widths: label1, value1, label2, value2
    w1 = 33*mm; w2 = CW/2 - w1; w3 = 33*mm; w4 = CW/2 - w3
    rows = [[
        Paragraph(r[0], s['small']),
        Paragraph(str(r[1]) if r[1] else '—', s['body']),
        Paragraph(r[2], s['small']),
        Paragraph(str(r[3]) if r[3] else '—', s['body']),
    ] for r in fields]
    t = Table(rows, colWidths=[w1, w2, w3, w4])
    t.setStyle(TableStyle([
        ('GRID',(0,0),(-1,-1),0.4,G5),
        ('ROWBACKGROUNDS',(0,0),(-1,-1),[WHITE,G4]),
        ('TOPPADDING',(0,0),(-1,-1),5),
        ('BOTTOMPADDING',(0,0),(-1,-1),5),
        ('LEFTPADDING',(0,0),(-1,-1),6),
        ('RIGHTPADDING',(0,0),(-1,-1),6),
        # Labels: small grey regular
        ('FONTNAME',(0,0),(0,-1),'Helvetica'),
        ('FONTNAME',(2,0),(2,-1),'Helvetica'),
        ('FONTSIZE',(0,0),(0,-1),7.5),
        ('FONTSIZE',(2,0),(2,-1),7.5),
        ('TEXTCOLOR',(0,0),(0,-1),G2),
        ('TEXTCOLOR',(2,0),(2,-1),G2),
        # Values: normal weight black
        ('FONTNAME',(1,0),(1,-1),'Helvetica'),
        ('FONTNAME',(3,0),(3,-1),'Helvetica'),
        ('FONTSIZE',(1,0),(1,-1),8),
        ('FONTSIZE',(3,0),(3,-1),8),
        ('TEXTCOLOR',(1,0),(1,-1),G1),
        ('TEXTCOLOR',(3,0),(3,-1),G1),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
    ]))
    return t


def _inv_block(s, inv):
    def g(k, default='—'):
        v = inv.get(k)
        return str(v).strip() if v not in (None, '', 'None') else default
    return [
        *_sec(s, "Investor's Information"),
        _inv_grid(s, [
            ["Account Type",    g('account_type','Nominee Account'),
             "Account ID",      g('account_id')],
            ["Registered Name", g('name'),
             "Settlement Type", "Banking"],
            ["Phone No.",       g('phone'),
             "Bank Name",       g('bank_name')],
            ["Email Address",   g('email'),
             "Bank Account No.",g('bank_account_no')],
            ["Nominee Name",    "—",
             "Total Days Held", f"{inv.get('days_held',0)} days"],
        ]),
        Spacer(1, 2*mm),
    ]


def _dtbl(s, headers, rows, col_w, total_row=None):
    """Data table — col_w must sum to CW."""
    def _c(v):
        if isinstance(v, Paragraph): return v
        return Paragraph(str(v) if v is not None else '—',
            ParagraphStyle('_c', fontName='Helvetica', fontSize=8, textColor=G1, leading=11))
    hrow = [Paragraph(h, ParagraphStyle('_h', fontName='Helvetica-Bold',
                fontSize=8, textColor=WHITE, alignment=TA_CENTER)) for h in headers]
    td = [hrow] + [[_c(c) for c in r] for r in rows]
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
    import matplotlib; matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    if not ntas: return None
    lo, hi = min(ntas), max(ntas)
    pad    = max((hi - lo) * 0.15, 0.05)
    y_min  = round((lo - pad) / 0.05) * 0.05
    y_max  = round((hi + pad) / 0.05 + 1) * 0.05
    DPI  = 192
    FS   = 9.0   # at DPI=192 renders crisp and readable
    w_in = width  / 72
    h_in = height / 72
    fig, ax = plt.subplots(figsize=(w_in, h_in), dpi=DPI)
    fig.subplots_adjust(top=0.95, bottom=0.15, left=0.09, right=0.98)

    x = list(range(len(dates)))
    ax.plot(x, ntas, color='#1565C0', linewidth=1.5, zorder=3)
    # No fill, no gridlines — clean line chart
    step = max(1, len(dates) // 10)
    ax.set_xticks(x[::step])
    ax.set_xticklabels([dates[i] for i in x[::step]], fontsize=FS * 0.85,
                        rotation=0, ha='center')
    ax.set_ylim(y_min, y_max)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter('%.3f'))
    ax.tick_params(axis='y', labelsize=FS, length=0, pad=3)
    ax.tick_params(axis='x', labelsize=FS * 0.9, length=0, pad=2)
    ax.grid(False)  # no gridlines
    for sp in ['top','right','bottom']: ax.spines[sp].set_visible(False)
    ax.spines['left'].set_color('#cccccc')
    ax.spines['left'].set_linewidth(0.5)

    # No data labels on line chart — clean

    buf = io.BytesIO()
    fig.savefig(buf, format='png', transparent=True,
                bbox_inches='tight', pad_inches=0.05, dpi=DPI)
    plt.close(fig); buf.seek(0)
    return Image(buf, width=width, height=height)


def _donut(labels, values, size=(55*mm,55*mm)):
    import matplotlib; matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    if not values or sum(values) == 0: return None
    fig, ax = plt.subplots(figsize=(2.2,2.2), dpi=150)
    _, _, autotexts = ax.pie(
        values, autopct=lambda p: f'{p:.1f}%' if p > 4 else '',
        pctdistance=0.75,
        colors=[CC[i%len(CC)] for i in range(len(values))],
        startangle=90,
        wedgeprops=dict(width=0.52, edgecolor='white', linewidth=0.3))
    for at in autotexts:
        at.set_fontsize(5.5); at.set_color('white'); at.set_fontweight('bold')
    ax.set_aspect('equal')
    plt.tight_layout(pad=0.1)
    buf = io.BytesIO()
    fig.savefig(buf, format='png', transparent=True, bbox_inches='tight', dpi=150)
    plt.close(fig); buf.seek(0)
    return Image(buf, width=size[0], height=size[1])


def _pie_chart(labels, values, width=55*mm):
    """Pie chart — perfect circle, centred horizontally, legend below."""
    import matplotlib; matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch
    if not values or sum(v for v in values if v > 0) == 0: return None

    colors = [CC[i % len(CC)] for i in range(len(labels))]
    DPI    = 192

    # Key insight: render a SQUARE figure for the pie so the circle is never distorted.
    # Use a fixed square size matching the column width, then add legend separately.
    w_in   = width / 72           # column width in inches

    # Step 1: draw pie on a square figure → perfect circle guaranteed
    fig_pie, ax = plt.subplots(figsize=(w_in, w_in), dpi=DPI)
    fig_pie.subplots_adjust(left=0, right=1, top=1, bottom=0)
    ax.pie(values, colors=colors, startangle=90,
           wedgeprops=dict(edgecolor='white', linewidth=1.2),
           counterclock=False)
    ax.set_aspect('equal')
    buf_pie = io.BytesIO()
    fig_pie.savefig(buf_pie, format='png', transparent=True,
                    bbox_inches='tight', pad_inches=0.02, dpi=DPI)
    plt.close(fig_pie); buf_pie.seek(0)

    # Step 2: draw legend on a separate short wide figure
    leg_r   = -(-len(labels) // 2)
    leg_h   = leg_r * 0.22
    fig_leg, ax_l = plt.subplots(figsize=(w_in, leg_h), dpi=DPI)
    fig_leg.subplots_adjust(left=0, right=1, top=1, bottom=0)
    ax_l.axis('off')
    handles = [Patch(facecolor=colors[i],
                     label=f'{labels[i]}  {values[i]:.1f}%')
               for i in range(len(labels))]
    ax_l.legend(handles=handles, loc='center', bbox_to_anchor=(0.5, 0.5),
                ncol=2, fontsize=4.0 * DPI / 72, frameon=False,
                handlelength=1.0, handleheight=0.8,
                columnspacing=0.8, labelspacing=0.3)
    buf_leg = io.BytesIO()
    fig_leg.savefig(buf_leg, format='png', transparent=True,
                    bbox_inches='tight', pad_inches=0.02, dpi=DPI)
    plt.close(fig_leg); buf_leg.seek(0)

    # Combine into a single flowable using a 1-cell table
    from reportlab.platypus import Table as _T, TableStyle as _TS
    pie_img = Image(buf_pie, width=width, height=width)
    leg_img = Image(buf_leg, width=width, height=leg_r * 6*mm)
    t = _T([[pie_img], [leg_img]], colWidths=[width])
    t.setStyle(_TS([
        ('ALIGN',    (0,0), (-1,-1), 'CENTER'),
        ('TOPPADDING',  (0,0), (-1,-1), 0),
        ('BOTTOMPADDING',(0,0), (-1,-1), 0),
        ('LEFTPADDING',  (0,0), (-1,-1), 0),
        ('RIGHTPADDING', (0,0), (-1,-1), 0),
    ]))
    return t


def _bar_chart(labels, values, width, height=60*mm):
    """Horizontal bar chart — bars top, colour legend below, crisp text."""
    import matplotlib; matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    from matplotlib.patches import Patch
    if not values or sum(values) == 0: return None

    n      = len(labels)
    colors = [CC[i % len(CC)] for i in range(n)]

    # Key insight: render at NATIVE PDF resolution
    # figsize in inches = pts/72  →  dpi=72  →  1px = 1pt → no scaling distortion
    # Use higher DPI only for antialiasing, but keep figsize consistent
    DPI    = 192                         # 2x retina → crisp in PDF viewer
    w_in   = width  / 72                 # exact PDF width in inches
    # Height: bars + legend rows (0.22in each)
    leg_r  = -(-n // 2)                 # ceil(n/2) legend rows
    h_bars = height / 72
    h_leg  = leg_r * 0.22
    h_in   = h_bars + h_leg

    # At DPI=96, figsize=(w_in, h_in): 1pt matplotlib = 1pt × (DPI/72) ≈ 1.33pt in PDF
    # → use fontsize = desired_pt / (DPI/72) = desired_pt × 72/DPI
    # For 7pt in PDF: mfs = 7 * 72/96 = 5.25 → round to 5
    # Actually simpler: just use small fonts and let bbox_inches='tight' handle layout
    FS = 9   # at DPI=192 renders crisp and readable in PDF

    fig, ax = plt.subplots(figsize=(w_in, h_in), dpi=DPI)
    # Leave bottom margin for legend
    fig.subplots_adjust(top=0.97, bottom=h_leg/h_in + 0.02, left=0.02, right=0.98)

    y_pos = list(range(n - 1, -1, -1))
    bars  = ax.barh(y_pos, values, color=colors, height=0.55, zorder=3, edgecolor='none')

    for bar, val in zip(bars, values):
        bw = bar.get_width()
        if bw > 10:
            ax.text(bw / 2, bar.get_y() + bar.get_height() / 2,
                    f'{val:.1f}%', ha='center', va='center',
                    fontsize=FS, color='white', fontweight='bold')
        elif bw > 0:
            ax.text(bw + max(values)*0.01, bar.get_y() + bar.get_height() / 2,
                    f'{val:.1f}%', ha='left', va='center',
                    fontsize=FS * 0.9, color='#444444')

    ax.set_yticks([])
    ax.set_xlim(0, max(values) * 1.08)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:.0f}%'))
    ax.tick_params(axis='x', labelsize=FS, length=2, pad=2)
    ax.grid(False)  # no gridlines
    for sp in ['top','right','left','bottom']: ax.spines[sp].set_visible(False)

    handles = [Patch(facecolor=colors[i], label=f'{labels[i]}  {values[i]:.1f}%')
               for i in range(n)]
    ax.legend(handles=handles, loc='upper center',
              bbox_to_anchor=(0.5, -(0.02 + (h_bars/h_in)*0.05)),
              ncol=2, fontsize=FS * 0.82, frameon=False,
              handlelength=1.0, handleheight=0.8,
              columnspacing=1.0, labelspacing=0.3)

    buf = io.BytesIO()
    fig.savefig(buf, format='png', transparent=True,
                bbox_inches='tight', pad_inches=0.05, dpi=DPI)
    plt.close(fig); buf.seek(0)
    return Image(buf, width=width, height=height + leg_r * 7*mm)


# ══════════════════════════════════════════════════════════════
# 1.  FACTSHEET  (no letter header — fund-wide report)
# ══════════════════════════════════════════════════════════════
def generate_factsheet(fund_data, holdings, performance, distributions,
                       nta_history, sector_data, manager_comment='',
                       as_of_date=None):
    s = S(); story = []

    # Resolve as_of date for subtitle
    if as_of_date:
        _ao = as_of_date if hasattr(as_of_date, 'strftime') else               __import__('datetime').date.fromisoformat(str(as_of_date))
    else:
        _ao = date.today()
    period = _ao.strftime('%d %B %Y')

    _justify = ParagraphStyle('_jus', parent=s['body'],
        alignment=TA_JUSTIFY, leading=13, spaceAfter=2)

    # ── Intro ─────────────────────────────────────────────────
    story.append(Spacer(1, 3*mm))
    story.append(Paragraph(
        'The portfolio aims to provide investors with capital appreciation higher than '
        'prevailing fixed-deposit rates by investing in a high-growth portfolio of '
        'equities and fixed income instruments.', _justify))
    story.append(Spacer(1, 4*mm))

    # ── PAGE 1: Fund Details | Sector Allocation ─────────────
    nta_v = fund_data.get('current_nta', 0)
    L_W   = 100*mm
    R_W   = CW - L_W - 4*mm

    fd_rows = [
        ('Manager',                'Mr. Ng Zhi Yao'),
        ('Fund Category',          'Equity Fund'),
        ('Launch Date',            '13 December 2021'),
        ('Unit Net Asset Value',   fm(nta_v, 4)),
        ('Fund Size',              fm(fund_data.get('aum', 0))),
        ('Units in Circulation',   fn(fund_data.get('total_units', 0), 2) + ' units'),
        ('Financial Year End',     '30 November'),
        ('Min. Initial Investment','RM 10,000.00'),
        ('Min. Additional',        'RM 1,000.00'),
        ('Benchmark',              'FBMKLCI, Tech Index'),
        ('Annual Mgmt Fee',        '0.10% p.a. of NAV'),
        ('Performance Fee',        '10.0% p.a. of excess return'),
        ('Distribution Policy',    'At least 80% of gross income'),
    ]
    fd_t = Table(
        [[Paragraph(k, s['small']), Paragraph(v, s['body'])]  for k, v in fd_rows],
        colWidths=[40*mm, L_W - 40*mm])
    fd_t.setStyle(TableStyle([
        ('ROWBACKGROUNDS', (0,0), (-1,-1), [WHITE, G4]),
        ('GRID',           (0,0), (-1,-1), 0.4, G5),
        ('TOPPADDING',     (0,0), (-1,-1), 3.5),
        ('BOTTOMPADDING',  (0,0), (-1,-1), 3.5),
        ('LEFTPADDING',    (0,0), (-1,-1), 5),
        ('FONTSIZE',       (0,0), (-1,-1), 7.5),
        ('TEXTCOLOR',      (0,0), (0,-1),  G2),
    ]))

    # Right: pie chart with legend below
    right = [Paragraph('Asset Allocation*', s['h3']),
             HRFlowable(width='100%', thickness=0.6, color=BLUE, spaceAfter=3)]
    if sector_data:
        lbls = [r.get('asset_class') or r.get('sector', '') for r in sector_data[:8]]
        vals = [max(float(r.get('weight_pct', 0)), 0)       for r in sector_data[:8]]
        pie  = _pie_chart(lbls, vals, width=min(R_W, 60*mm))
        if pie: right.append(pie)
    else:
        right.append(Paragraph('No data', s['small']))
    right.append(Paragraph('* As % of NAV', s['tiny']))

    def _col(items, w):
        t = Table([[it] for it in items], colWidths=[w])
        t.setStyle(TableStyle([('LEFTPADDING',(0,0),(-1,-1),0),
            ('RIGHTPADDING',(0,0),(-1,-1),0),
            ('TOPPADDING',(0,0),(-1,-1),0),
            ('BOTTOMPADDING',(0,0),(-1,-1),1)]))
        return t

    left_items = [Paragraph('Fund Details', s['h3']),
                  HRFlowable(width='100%', thickness=0.6, color=BLUE, spaceAfter=3), fd_t]
    two_col = Table([[_col(left_items, L_W), Spacer(4*mm,1), _col(right, R_W)]],
                    colWidths=[L_W, 4*mm, R_W])
    two_col.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),
        ('LEFTPADDING',(0,0),(-1,-1),0), ('RIGHTPADDING',(0,0),(-1,-1),0),
        ('TOPPADDING',(0,0),(-1,-1),0),  ('BOTTOMPADDING',(0,0),(-1,-1),0)]))
    story.append(two_col)
    story.append(Spacer(1, 4*mm))

    # ── Portfolio Performance Analysis (Page 1) ───────────────
    if nta_history:
        story += _sec(s, 'Portfolio Performance Analysis')
        dates, ntas = [], []
        for r in nta_history:
            try:
                dt = datetime.fromisoformat(str(r.get('date', '')))
                dates.append(dt.strftime('%m/%y'))
                ntas.append(float(r.get('nta', 1)))
            except: pass
        if ntas:
            ch = _nta_chart(dates, ntas, width=CW, height=62*mm)
            if ch: story.append(ch); story.append(Spacer(1, 2*mm))
        perf      = performance.get('period_returns', {})
        total_ret = performance.get('total_return_pct',
                    fund_data.get('total_return_pct', 0))
        c1 = 28*mm; cn = round((CW - c1) / 6)
        story.append(_dtbl(s,
            headers=['', '1M', '3M', '6M', '1Y', '3Y', 'All'],
            rows=[['Portfolio'] + [fp(perf.get(k)) for k in ['1M','3M','6M','1Y','3Y']]
                  + [fp(total_ret)]],
            col_w=[c1, cn, cn, cn, cn, cn, CW - c1 - cn*5]))
        story.append(Spacer(1, 4*mm))

    # ── PAGE 2 ────────────────────────────────────────────────
    story.append(PageBreak())

    # Investment Strategy
    story += _sec(s, 'Investment Strategy')
    _strat_intro = ParagraphStyle('_si', parent=s['body'],
        alignment=TA_JUSTIFY, leading=13, spaceAfter=3)
    _bullet_st = ParagraphStyle('_bs', parent=s['body'],
        leftIndent=8*mm, spaceAfter=2, leading=13)
    story.append(Paragraph(
        'The strategic limit on asset allocation of the fund is as follows:',
        _strat_intro))
    for pt in [
        '• Equities: Minimum 60% and maximum 98%.',
        '• Fixed-income securities: Minimum 2% and maximum 40%.',
        '• Derivatives: Maximum 5%.  (Subject to Shareholder Approval)',
    ]:
        story.append(Paragraph(pt, _bullet_st))
    story.append(Spacer(1, 4*mm))

    # Distribution History
    if distributions:
        story += _sec(s, 'Distribution History')
        story.append(_dtbl(s,
            headers=['Year', 'Description', 'DPS (sen)', 'Payout Ratio'],
            rows=[[d.get('financial_year', '—'), d.get('title', '—'),
                   fn(d.get('dps_sen'), 2) + ' sen',
                   f"{float(d['payout_ratio']):.1f}%" if d.get('payout_ratio') else '—']
                  for d in distributions],
            col_w=[20*mm, CW - 78*mm, 30*mm, 28*mm]))
        story.append(Spacer(1, 4*mm))

    # Largest Holdings
    if holdings:
        story += _sec(s, 'Largest Holdings*')
        story.append(_dtbl(s,
            headers=['Asset Name', 'Percentage'],
            rows=[[h.get('instrument', '—'), f"{float(h.get('weight_pct', 0)):.2f}%"]
                  for h in holdings[:10]],
            col_w=[CW - 30*mm, 30*mm]))
        story.append(Paragraph(
            '* As percentage of Net Asset Value (NAV) of the fund', s['tiny']))
        story.append(Spacer(1, 4*mm))

    # Manager's Comments
    story += _sec(s, "Manager's Comments")
    story.append(Paragraph(manager_comment or
        'Our portfolio maintains its current strategic positions. We continue to '
        'monitor market developments and macroeconomic conditions closely.', _justify))
    story.append(Spacer(1, 3*mm))

    # Disclaimer
    story.append(HRFlowable(width=CW, thickness=0.5, color=G5))
    story.append(Spacer(1, 2*mm))
    story.append(Paragraph('<b>Disclaimer</b>',
        ParagraphStyle('_d', fontName='Helvetica-Bold', fontSize=8, textColor=G1)))
    story.append(Paragraph(
        'Investment involves significant risk, including the potential loss of principal. '
        'Past performance is not indicative of future results. This update is intended '
        'strictly for family members only and does not constitute a financial prospectus.',
        s['notice']))

    return _build(story, title='MONTHLY FACTSHEET', subtitle=period,
                  name='', addr='', meta_rows=None, extra_top=0)
# ── Personal statement builder ──────────────────────────────────
def _personal(story, inv, title, issued, stmt_type, stmt_period, page='1 of 1'):
    """Build and return PDF bytes for a personal statement."""
    s      = S()
    name   = inv.get('name','')
    addr   = _addr(inv)
    meta   = _meta(page, issued, stmt_type, stmt_period)
    extra  = _letter_height(name, addr, meta) + 4*mm
    return _build(story, title=title, subtitle='', name=name, addr=addr,
                  meta_rows=meta, extra_top=extra)


# ══════════════════════════════════════════════════════════════
# 2.  SUBSCRIPTION STATEMENT
# ══════════════════════════════════════════════════════════════
def generate_subscription(investor, cashflow_record):
    """
    Opening  = cumulative position BEFORE this subscription
    This row = new subscription (no avg cost shown — NTA is the price paid)
    Closing  = cumulative position AFTER (total invested, total units, new VWAP)
    """
    s = S(); story = []
    today    = date.today()
    inv_date = cashflow_record.get('date','')
    amt      = abs(float(cashflow_record.get('amount',0)))
    units    = float(cashflow_record.get('units',0))
    nta_d    = cashflow_record.get('nta_at_date')

    # Compute opening balance from prior cashflows (VWAP method)
    prior = cashflow_record.get('prior_cashflows', [])
    op_units = 0.0; op_cost = 0.0
    for cf in prior:
        u = float(cf.get('units', 0))
        a = float(cf.get('amount', 0))
        if a > 0:
            # Subscription: add units and cost
            op_units += u
            op_cost  += abs(a)
        elif a < 0:
            # Redemption: remove units; reduce cost at current VWAP
            avg = op_cost / op_units if op_units > 0 else 0
            redeemed = abs(u)
            op_units = max(0.0, op_units - redeemed)
            op_cost  = max(0.0, op_cost - redeemed * avg)
    op_avg = (op_cost / op_units) if op_units > 0 else 0

    # Closing balance
    cl_units = op_units + units
    cl_cost  = op_cost  + amt
    cl_avg   = (cl_cost / cl_units) if cl_units > 0 else 0

    story += _inv_block(s, investor)
    story += _sec(s, 'Principal Transaction')
    c1,c2,c3,c4,c5,c6 = 26*mm,44*mm,32*mm,30*mm,28*mm,CW-160*mm
    story.append(_dtbl(s,
        headers=['Date','Description','Investment Value','Entry Price','Unit Balanced','Avg. Cost (VWAP)'],
        rows=[
            # Opening: prior cumulative position
            [fd(inv_date), 'Opening',
             fm(op_cost) if op_cost > 0 else '—',
             '—',
             fn(op_units,4) if op_units > 0 else '—',
             fn(op_avg,4)   if op_units > 0 else '—'],
            # This subscription: no avg cost (it IS the subscription price)
            [fd(inv_date), 'Fund Subscription',
             fm(amt),
             fn(nta_d,4) if nta_d else '—',
             fn(units,4),
             '—'],
            # Closing: new cumulative
            [fd(inv_date), 'Closing',
             fm(cl_cost),
             '—',
             fn(cl_units,4),
             fn(cl_avg,4)],
        ], col_w=[c1,c2,c3,c4,c5,c6]))

    return _personal(story, investor,
        title='FUND SUBSCRIPTION STATEMENT',
        issued=today.strftime('%d-%m-%Y'),
        stmt_type='Daily Statement',
        stmt_period=fd(inv_date))


# ══════════════════════════════════════════════════════════════
# 3.  REDEMPTION STATEMENT
# ══════════════════════════════════════════════════════════════
def generate_redemption(investor, cashflow_record):
    """
    Redemption value = units_redeemed × NTA_at_date
    Cost basis       = units_redeemed × avg_cost_before_redemption
    Realized P&L     = Redemption value − Cost basis
    Closing shows remaining units and remaining invested cost.
    """
    s = S(); story = []
    today    = date.today()
    inv_date = cashflow_record.get('date','')
    units_r  = abs(float(cashflow_record.get('units',0)))   # units redeemed (positive)
    nta_d    = cashflow_record.get('nta_at_date')

    # From redemption_ledger (passed by admin.py when available)
    avg_cost_pre = float(cashflow_record.get('avg_cost_at_redemption') or 0)
    redeem_value = float(cashflow_record.get('redemption_value') or 0)
    cost_basis   = float(cashflow_record.get('cost_basis') or 0)
    realized_pl  = float(cashflow_record.get('realized_pl') or 0)

    # Compute redeem_value from NTA if not provided
    nta_f = float(nta_d) if nta_d else 0
    if redeem_value == 0 and nta_f > 0:
        redeem_value = units_r * nta_f

    # Opening: compute from prior cashflows (VWAP method)
    prior = cashflow_record.get('prior_cashflows', [])
    op_units = 0.0; op_cost = 0.0
    for cf in prior:
        u = float(cf.get('units', 0)); a = float(cf.get('amount', 0))
        if a > 0:
            op_units += u; op_cost += abs(a)
        elif a < 0:
            avg = op_cost / op_units if op_units > 0 else 0
            op_units = max(0.0, op_units - abs(u))
            op_cost  = max(0.0, op_cost  - abs(u) * avg)
    op_avg = (op_cost / op_units) if op_units > 0 else avg_cost_pre

    # Use op_avg as the definitive VWAP; recompute cost_basis and realized_pl if needed
    if op_avg > 0 and cost_basis == 0:
        cost_basis  = units_r * op_avg
        realized_pl = redeem_value - cost_basis

    # AVCO: closing cost = opening cost minus cost_basis of redeemed units
    # Average cost DOES NOT CHANGE after redemption under AVCO method
    cl_units = max(0, op_units - units_r)
    cl_cost  = max(0, op_cost  - cost_basis)   # reduces by cost basis, not redeem value
    cl_avg   = op_avg   # AVCO: avg cost unchanged by redemption

    pl_color = GREEN if realized_pl >= 0 else RED

    story += _inv_block(s, investor)

    # ── Redemption Transaction table ─────────────────────────
    story += _sec(s, 'Redemption Transaction')
    c1,c2,c3,c4,c5,c6 = 26*mm,44*mm,32*mm,30*mm,28*mm,CW-160*mm
    story.append(_dtbl(s,
        headers=['Date','Description','Investment Value','NTA / Entry','Units','Avg. Cost (VWAP)'],
        rows=[
            [fd(inv_date), 'Opening',
             fm(op_cost)  if op_cost  > 0 else '—', '—',
             fn(op_units,4) if op_units > 0 else '—',
             fn(op_avg,4)   if op_units > 0 else '—'],
            [fd(inv_date), 'Fund Redemption',
             fm(redeem_value),
             fn(nta_d,4) if nta_d else '—',
             fn(units_r,4),
             fn(op_avg,4) if op_avg else '—'],   # AVCO = opening avg cost
            [fd(inv_date), 'Closing',
             fm(cl_cost), '—',                    # op_cost - cost_basis (AVCO)
             fn(cl_units,4),
             fn(cl_avg,4) if cl_units > 0 else '—'],  # unchanged under AVCO
        ], col_w=[c1,c2,c3,c4,c5,c6]))

    # ── Realized P&L breakdown ────────────────────────────────
    story += _sec(s, 'Realized Profit & Loss')
    pl_sign = '+' if realized_pl >= 0 else ''
    story.append(_dtbl(s,
        headers=['Description','Units Redeemed','Avg. Cost (VWAP)','NTA at Redemption','Cost Basis','Realized P&L'],
        rows=[[
            'Fund Redemption',
            fn(units_r,4),
            fn(op_avg,4) if op_avg else (fn(avg_cost_pre,4) if avg_cost_pre else '—'),
            fn(nta_d,4) if nta_d else '—',
            fm(cost_basis),
            Paragraph(f'<b>{pl_sign}{fm(realized_pl)}</b>',
                ParagraphStyle('_rpl', fontName='Helvetica-Bold', fontSize=8,
                               textColor=pl_color, alignment=TA_RIGHT)),
        ]],
        col_w=[40*mm,26*mm,24*mm,28*mm,26*mm,CW-144*mm]))
    story.append(Paragraph(
        'Formula: Realized P&L = (NTA at Redemption − Average Cost) × Units Redeemed',
        s['tiny']))

    return _personal(story, investor,
        title='FUND REDEMPTION STATEMENT',
        issued=today.strftime('%d-%m-%Y'),
        stmt_type='Daily Statement',
        stmt_period=fd(inv_date))


# ══════════════════════════════════════════════════════════════
# 4.  DIVIDEND PAYMENT STATEMENT
# ══════════════════════════════════════════════════════════════
def generate_dividend_statement(investor, dist_record, financial_year):
    s = S(); story = []
    today = date.today()
    dps   = float(dist_record.get('dps_sen',0))
    units = float(dist_record.get('units_at_ex_date',0))
    amt   = float(dist_record.get('amount',0))
    eps   = dist_record.get('eps')
    dpr   = dist_record.get('payout_ratio')
    pmt   = dist_record.get('pmt_date') or dist_record.get('ex_date')
    lbl   = dist_record.get('title', dist_record.get('dist_type', financial_year))

    # Investor info (3-row version for dividend)
    story += [
        *_sec(s, "Investor's Information"),
        _inv_grid(s,[
            ["Investor's Name", investor.get('name','—'), "Settlement Type",   "Banking"],
            ["Phone Number",    investor.get('phone','—'),"Bank Name",         investor.get('bank_name','—')],
            ["Email Address",   investor.get('email','—'),"Bank Account No.",  investor.get('bank_account_no','—')],
        ]),
        Spacer(1,5*mm),
    ]
    story += _sec(s, 'Dividend Details')
    # 7 cols: 26+44+26+16+16+20+CW-148 = CW
    c1,c2,c3,c4,c5,c6,c7 = 26*mm,44*mm,26*mm,16*mm,16*mm,20*mm,CW-148*mm
    story.append(_dtbl(s,
        headers=['Date','Description','Holding Units','EPS','DPR','DPS (sen)','Amount'],
        rows=[[fd(pmt), lbl, fn(units,4),
               fn(eps,2) if eps else '—',
               f"{float(dpr):.1f}%" if dpr else '—',
               fn(dps,2),
               Paragraph(f'<b>{fm(amt)}</b>',
                   ParagraphStyle('_am', fontName='Helvetica-Bold', fontSize=8,
                                  textColor=BLUE, alignment=TA_RIGHT))]],
        col_w=[c1,c2,c3,c4,c5,c6,c7],
        total_row=['','Total','','','',fn(dps,2),
            Paragraph(f'<b>{fm(amt)}</b>',
                ParagraphStyle('_ta', fontName='Helvetica-Bold', fontSize=8,
                               textColor=BLUE, alignment=TA_RIGHT))]))
    story.append(Paragraph(
        'Notes: EPS = Earning Per Share  |  DPR = Dividend Payout Ratio  |  DPS = Dividend Per Share (sen)',
        s['tiny']))

    return _personal(story, investor,
        title='DIVIDEND PAYMENT STATEMENT',
        issued=today.strftime('%d-%m-%Y'),
        stmt_type=financial_year,
        stmt_period=fd(pmt))


# ══════════════════════════════════════════════════════════════
# 5.  INVESTMENT ACCOUNT STATEMENT  (2 pages)
# ══════════════════════════════════════════════════════════════
def generate_account_statement(investor, summary, cashflows, dist_history,
                                statement_period, financial_year):
    s = S(); story = []
    today = date.today()
    name  = investor.get('name','')
    addr  = _addr(investor)
    # Estimate extra_top with a placeholder meta (page count not known yet)
    _meta_placeholder = _meta('1 of ?', today.strftime('%d-%m-%Y'), 'Annually', statement_period)
    extra = _letter_height(name, addr, _meta_placeholder) + 4*mm

    # ── Page 1: Account Summary ──────────────────────────────
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

    # Account summary table — 5 cols: 9+84+28+28+CW-149
    c1,c2,c3,c4,c5 = 9*mm,84*mm,28*mm,28*mm,CW-149*mm
    hdr = [Paragraph(h, ParagraphStyle('_sh', fontName='Helvetica-Bold',
        fontSize=8, textColor=WHITE, alignment=TA_CENTER))
        for h in ['','Fields','Holding Units','Average Price','Total Value (RM)']]
    rows_data = [
        [_pr('(a)'), _pr('Latest Fund Price'),                _pr(fn(units_v,4),TA_RIGHT), _pr(fn(nta_v,4),TA_RIGHT),  _pr(fm(mv_v),TA_RIGHT)],
        [_pr('(b)'), _pr('Subscription Cost'),                _pr(fn(units_v,4),TA_RIGHT), _pr(fn(avg_c,4),TA_RIGHT),  _pr(f'({fm(cost_v)})',TA_RIGHT)],
        [_pr('(c)'), _pr('Unrealized P&L:  (a) + (b)'),      _pr(''),                      _pr(''),                     _pr(fm(unr_v),TA_RIGHT)],
        [_pr('(d)'), _pr('Realized P&L  (from Redemptions Only)'), _pr(''), _pr(''), _pr(fm(rea_v) if rea_v else '—',TA_RIGHT)],
        [_pr('(e)'), _pr('Dividend Received'),                _pr(''),                      _pr(''),                     _pr(fm(div_v) if div_v else '—',TA_RIGHT)],
        [_pr('(f)'), _pr(f'Adjustment / Fund Switching {fm(abs(adj_v))}'),
                                                              _pr(''),                      _pr(''),
            _pr(f'({fm(abs(adj_v))})' if adj_v < 0 else (fm(adj_v) if adj_v else '—'),TA_RIGHT)],
    ]
    td = [hdr] + rows_data + [
        [_pr(''), _bl('Total Profit & Loss:  (c)+(d)+(e)+(f)'), _pr(''), _pr(''), _bcr(fm(tpl), tc)],
        [_pr(''), _bl('Total Performance %'),                   _pr(''), _pr(''), _bcr(fp(tpct), tpc)],
        [_pr(''), _bl('Annualised Performance* %'),             _pr(''), _pr(''), _bcr(fp(irr) if irr else '—', ic)],
    ]
    sum_t = Table(td, colWidths=[c1,c2,c3,c4,c5])
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

    story += _inv_block(s, investor)
    story += _sec(s, 'Account Summary')
    story.append(sum_t)
    story.append(Paragraph(
        "* Powered by Internal Rate of Return (IRR) & Newton's method", s['tiny']))

    # ── Page 2 ─────────────────────────────────────────────────
    story.append(PageBreak())

    if cashflows:
        cf_rows = []; running = 0.0
        for cf in cashflows:
            u = float(cf.get('units',0)); amt = float(cf.get('amount',0))
            running += u; nta_d = cf.get('nta_at_date')
            cf_rows.append([fd(cf.get('date')), cf.get('description','—'),
                fm(abs(amt)) if amt else '—', fm(nta_d) if nta_d else '—',
                fn(abs(u),4) if u else '—', fn(running,4) if running > 0 else '—'])
        # 6 cols: 24+50+34+26+24+CW-158
        cc1,cc2,cc3,cc4,cc5,cc6 = 24*mm,50*mm,34*mm,26*mm,24*mm,CW-158*mm
        story += _sec(s, 'Principal Transaction')
        story.append(_dtbl(s,
            headers=['Date','Description','Cashflow','Avg. Cost (RM)','Units Issued','Units Balanced'],
            rows=cf_rows, col_w=[cc1,cc2,cc3,cc4,cc5,cc6]))

    if dist_history:
        dc1,dc2,dc3,dc4,dc5,dc6 = 24*mm,50*mm,22*mm,30*mm,30*mm,CW-156*mm
        story += _sec(s, 'Distribution Transaction')
        story.append(_dtbl(s,
            headers=['Date','Description','DPS (sen)','Holding Units','Amount','Balanced (RM)'],
            rows=[[fd(d.get('pmt_date') or d.get('ex_date')), d.get('title','—'),
                   fn(d.get('dps_sen'),2),
                   fn(d.get('units_at_ex_date'),4) if d.get('units_at_ex_date') else '—',
                   fm(d.get('amount')) if d.get('amount') else '—',
                   fm(d.get('amount')) if d.get('amount') else '—']
                  for d in dist_history],
            col_w=[dc1,dc2,dc3,dc4,dc5,dc6]))

    # Two-pass build with per-page meta (page number changes per page)
    from reportlab.platypus import NextPageTemplate
    n   = _count_pages(story, extra)
    buf = io.BytesIO()
    doc = BaseDocTemplate(buf, pagesize=A4, leftMargin=LM, rightMargin=RM,
                          topMargin=H - BODY_TOP + extra, bottomMargin=BODY_BOT)
    fr  = Frame(LM, BODY_BOT, CW, BODY_H - extra, id='b',
                leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0)

    # Build page-specific meta AFTER knowing n
    def _make_meta(pg):
        return _meta(f'{pg} of {n}', today.strftime('%d-%m-%Y'), 'Annually', statement_period)

    def _cb_acct(canvas, doc):
        pg = doc.page
        _page_cb(n, title='INVESTMENT ACCOUNT STATEMENT',
                 name=name, addr=addr, meta_rows=_make_meta(pg))(canvas, doc)

    doc.addPageTemplates([PageTemplate(id='m', frames=[fr], onPage=_cb_acct)])
    doc.build(story)
    return buf.getvalue()
