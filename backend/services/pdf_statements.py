"""
ZY-Invest PDF Statement Generator  v1.0.0
Generates 4 statement types using ReportLab:
  1. Factsheet          (fund-wide)
  2. Subscription       (per investor)
  3. Dividend Payment   (per investor)
  4. Investment Account (per investor, annual)
"""
import io
import os
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, Image, KeepTogether
)
from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT

# ── Brand colours ─────────────────────────────────────────────
BLUE        = colors.HexColor('#1565C0')
BLUE_LIGHT  = colors.HexColor('#E3F0FF')
GREEN       = colors.HexColor('#2E7D32')
ORANGE      = colors.HexColor('#E65100')
GRAY_DARK   = colors.HexColor('#1F2937')
GRAY_MED    = colors.HexColor('#6B7280')
GRAY_LIGHT  = colors.HexColor('#F3F4F6')
BORDER      = colors.HexColor('#E5E7EB')
WHITE       = colors.white
RED         = colors.HexColor('#DC2626')

W, H = A4  # 595 x 842 pts

LOGO_PATH = os.path.join(os.path.dirname(__file__), '../../assets/img/logo_zy.png')

def fmt_rm(val) -> str:
    if val is None: return '—'
    try:
        v = float(val)
        return f"RM {v:,.2f}"
    except: return '—'

def fmt_num(val, dp=4) -> str:
    if val is None: return '—'
    try: return f"{float(val):,.{dp}f}"
    except: return '—'

def fmt_date(val) -> str:
    if val is None: return '—'
    if isinstance(val, (date, datetime)):
        return val.strftime('%d %b %Y')
    try:
        return datetime.fromisoformat(str(val)).strftime('%d %b %Y')
    except: return str(val)

def fmt_pct(val) -> str:
    if val is None: return '—'
    try: return f"{float(val):+.2f}%"
    except: return '—'


# ── Shared styles ──────────────────────────────────────────────
def get_styles():
    base = getSampleStyleSheet()
    s = {}
    s['title'] = ParagraphStyle('title',
        fontName='Helvetica-Bold', fontSize=16,
        textColor=GRAY_DARK, spaceAfter=2)
    s['subtitle'] = ParagraphStyle('subtitle',
        fontName='Helvetica', fontSize=10,
        textColor=GRAY_MED, spaceAfter=6)
    s['section'] = ParagraphStyle('section',
        fontName='Helvetica-Bold', fontSize=9,
        textColor=BLUE, spaceBefore=10, spaceAfter=4,
        borderPadding=(3,0,3,0))
    s['body'] = ParagraphStyle('body',
        fontName='Helvetica', fontSize=8.5,
        textColor=GRAY_DARK, leading=13)
    s['small'] = ParagraphStyle('small',
        fontName='Helvetica', fontSize=7.5,
        textColor=GRAY_MED, leading=11)
    s['bold'] = ParagraphStyle('bold',
        fontName='Helvetica-Bold', fontSize=8.5,
        textColor=GRAY_DARK)
    s['right'] = ParagraphStyle('right',
        fontName='Helvetica', fontSize=8.5,
        textColor=GRAY_DARK, alignment=TA_RIGHT)
    s['right_bold'] = ParagraphStyle('right_bold',
        fontName='Helvetica-Bold', fontSize=8.5,
        textColor=GRAY_DARK, alignment=TA_RIGHT)
    s['center'] = ParagraphStyle('center',
        fontName='Helvetica', fontSize=8.5,
        textColor=GRAY_DARK, alignment=TA_CENTER)
    s['notice_title'] = ParagraphStyle('notice_title',
        fontName='Helvetica-Bold', fontSize=8,
        textColor=GRAY_DARK, spaceBefore=4)
    s['notice'] = ParagraphStyle('notice',
        fontName='Helvetica', fontSize=7.5,
        textColor=GRAY_MED, leading=11)
    return s


def build_header(story, s, title: str, issued_date: str,
                 page_info: str = "Page 1 of 1",
                 statement_type: str = "",
                 statement_period: str = "",
                 investor_name: str = "",
                 investor_address: str = ""):
    """Build the standard ZY-Invest statement header."""

    # Logo + right-side info table
    logo_cell = ""
    if os.path.exists(LOGO_PATH):
        try:
            logo = Image(LOGO_PATH, width=22*mm, height=18*mm)
            logo_cell = logo
        except:
            logo_cell = Paragraph("<b>ZY-Invest</b>", s['title'])
    else:
        logo_cell = Paragraph("<b>ZY-Invest</b>", s['title'])

    right_data = [
        [Paragraph(f"<b>{title}</b>", ParagraphStyle('th',
            fontName='Helvetica-Bold', fontSize=12,
            textColor=GRAY_DARK, alignment=TA_RIGHT))],
    ]
    header_table = Table(
        [[logo_cell, Table(right_data, colWidths=[110*mm])]],
        colWidths=[50*mm, 115*mm]
    )
    header_table.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('ALIGN',  (1,0), (1,0),  'RIGHT'),
    ]))
    story.append(header_table)
    story.append(Spacer(1, 3*mm))
    story.append(HRFlowable(width="100%", thickness=1.5, color=BLUE))
    story.append(Spacer(1, 3*mm))

    # Address block + right meta
    if investor_name:
        left_lines = [
            Paragraph(f"<b>{investor_name}</b>", s['body']),
        ]
        if investor_address:
            for line in investor_address.split('\n'):
                if line.strip():
                    left_lines.append(Paragraph(line.strip(), s['small']))

        meta_rows = [
            ['Page No.', f': {page_info}'],
            ['Issued Date', f': {issued_date}'],
        ]
        if statement_type:
            meta_rows.append(['Statement Type', f': {statement_type}'])
        if statement_period:
            meta_rows.append(['Statement Period', f': {statement_period}'])
        meta_rows += [
            ['Email Address', ': nzy.invest@gmail.com'],
            ['Telephone No.', ': (+60)11 - 1121 8085'],
        ]

        meta_table = Table(
            [[Paragraph(r[0], s['small']),
              Paragraph(r[1], s['small'])] for r in meta_rows],
            colWidths=[32*mm, 78*mm]
        )
        meta_table.setStyle(TableStyle([
            ('ALIGN',   (0,0), (-1,-1), 'LEFT'),
            ('VALIGN',  (0,0), (-1,-1), 'TOP'),
            ('TOPPADDING', (0,0), (-1,-1), 1),
            ('BOTTOMPADDING', (0,0), (-1,-1), 1),
        ]))

        addr_block = Table(
            [[[*left_lines], meta_table]],
            colWidths=[70*mm, 115*mm]
        )
        addr_block.setStyle(TableStyle([
            ('VALIGN', (0,0), (-1,-1), 'TOP'),
            ('ALIGN',  (1,0), (1,0), 'RIGHT'),
        ]))
        story.append(addr_block)
        story.append(Spacer(1, 4*mm))


def build_investor_info_table(story, s, fields: list):
    """Render a 2x2 bordered investor information table."""
    story.append(Paragraph("Investor's Information", s['section']))

    # Split into pairs for 2-column layout
    rows = []
    for i in range(0, len(fields), 2):
        row = []
        for j in range(2):
            if i+j < len(fields):
                k, v = fields[i+j]
                row.extend([
                    Paragraph(k, s['small']),
                    Paragraph(str(v) if v else '—', s['body'])
                ])
            else:
                row.extend([Paragraph('', s['small']), Paragraph('', s['body'])])
        rows.append(row)

    t = Table(rows, colWidths=[32*mm, 55*mm, 32*mm, 46*mm])
    t.setStyle(TableStyle([
        ('GRID',        (0,0), (-1,-1), 0.5, BORDER),
        ('BACKGROUND',  (0,0), (-1,-1), WHITE),
        ('ROWBACKGROUNDS', (0,0), (-1,-1), [WHITE, GRAY_LIGHT]),
        ('TOPPADDING',  (0,0), (-1,-1), 5),
        ('BOTTOMPADDING',(0,0),(-1,-1), 5),
        ('LEFTPADDING', (0,0), (-1,-1), 6),
        ('RIGHTPADDING',(0,0), (-1,-1), 6),
        ('VALIGN',      (0,0), (-1,-1), 'MIDDLE'),
        ('FONTNAME',    (0,0), (0,-1), 'Helvetica'),
        ('TEXTCOLOR',   (0,0), (0,-1), GRAY_MED),
        ('FONTNAME',    (2,0), (2,-1), 'Helvetica'),
        ('TEXTCOLOR',   (2,0), (2,-1), GRAY_MED),
        ('FONTSIZE',    (0,0), (-1,-1), 8),
    ]))
    story.append(t)
    story.append(Spacer(1, 4*mm))


def build_data_table(story, s, headers: list, rows: list,
                     col_widths: list, title: str = "",
                     total_row: list = None):
    """Generic styled data table."""
    if title:
        story.append(Paragraph(title, s['section']))

    header_row = [Paragraph(h, ParagraphStyle('th',
        fontName='Helvetica-Bold', fontSize=8,
        textColor=WHITE, alignment=TA_CENTER)) for h in headers]

    table_data = [header_row]
    for row in rows:
        table_data.append([
            Paragraph(str(c) if c is not None else '—',
                ParagraphStyle('td', fontName='Helvetica', fontSize=8,
                    textColor=GRAY_DARK, leading=11))
            for c in row
        ])

    if total_row:
        table_data.append([
            Paragraph(str(c) if c is not None else '',
                ParagraphStyle('tot', fontName='Helvetica-Bold', fontSize=8,
                    textColor=GRAY_DARK, alignment=TA_RIGHT if i > 0 else TA_LEFT))
            for i, c in enumerate(total_row)
        ])

    t = Table(table_data, colWidths=col_widths, repeatRows=1)
    n = len(table_data)
    style = [
        ('BACKGROUND',   (0,0), (-1,0), BLUE),
        ('TEXTCOLOR',    (0,0), (-1,0), WHITE),
        ('GRID',         (0,0), (-1,-1), 0.4, BORDER),
        ('ROWBACKGROUNDS',(0,1),(-1,-1 if not total_row else -2),
                         [WHITE, GRAY_LIGHT]),
        ('TOPPADDING',   (0,0), (-1,-1), 5),
        ('BOTTOMPADDING',(0,0), (-1,-1), 5),
        ('LEFTPADDING',  (0,0), (-1,-1), 6),
        ('RIGHTPADDING', (0,0), (-1,-1), 6),
        ('VALIGN',       (0,0), (-1,-1), 'MIDDLE'),
    ]
    if total_row:
        style += [
            ('BACKGROUND', (0,-1), (-1,-1), BLUE_LIGHT),
            ('LINEABOVE',  (0,-1), (-1,-1), 1, BLUE),
            ('FONTNAME',   (0,-1), (-1,-1), 'Helvetica-Bold'),
        ]
    t.setStyle(TableStyle(style))
    story.append(t)
    story.append(Spacer(1, 4*mm))


def build_footer(story, s):
    """Standard footer with important notices + footer line."""
    story.append(Spacer(1, 4*mm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=BORDER))
    story.append(Spacer(1, 3*mm))
    story.append(Paragraph("<b>IMPORTANT NOTICES</b>", s['notice_title']))
    notices = [
        "<b>Confidentiality:</b> This statement contains personal data and is intended solely for the recipient. "
        "Please do not share this document with any third parties.",
        "<b>Discrepancies:</b> Please review all figures carefully. Any discrepancies or \"untally\" figures must be "
        "reported to us immediately; failure to do so may result in the recipient bearing any associated losses.",
        "<b>Digital Statements:</b> Effective 1st January 2026, all future portfolio statements will be provided "
        "exclusively via WhatsApp.",
    ]
    for i, n in enumerate(notices, 1):
        story.append(Paragraph(f"{i}. {n}", s['notice']))

    story.append(Spacer(1, 3*mm))
    footer_table = Table([[
        Paragraph("Head Office: None", s['small']),
        Paragraph("Line: (60)11-1121 8085   Email: nzy.invest@gmail.com", s['small']),
    ]], colWidths=[50*mm, 115*mm])
    footer_table.setStyle(TableStyle([
        ('ALIGN', (1,0), (1,0), 'RIGHT'),
        ('TOPPADDING', (0,0), (-1,-1), 0),
    ]))
    story.append(footer_table)


# ─────────────────────────────────────────────────────────────
# 1. FACTSHEET
# ─────────────────────────────────────────────────────────────
def generate_factsheet(fund_data: dict, holdings: list,
                       performance: dict, distributions: list) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
        leftMargin=15*mm, rightMargin=15*mm,
        topMargin=12*mm, bottomMargin=12*mm)
    s = get_styles()
    story = []

    today = date.today()
    period = today.strftime('%B %Y')

    # Header
    build_header(story, s,
        title=f"ZY Family Vision Portfolio — {period}",
        issued_date=today.strftime('%d-%m-%Y'))

    story.append(Paragraph(
        "The portfolio aims to provide our investors with capital appreciation higher than the prevailing "
        "fixed-deposit rate by investing in a high-growth portfolio of stocks and fixed income instruments.",
        s['body']))
    story.append(Spacer(1, 4*mm))

    # Fund details + sector allocation side by side
    nta    = fund_data.get('current_nta', 0)
    aum    = fund_data.get('aum', 0)
    units  = fund_data.get('total_units', 0)
    t_ret  = fund_data.get('total_return_pct', 0)
    t_days = fund_data.get('trading_days', 0)

    fund_rows = [
        ['Manager',             'Mr. Ng Zhi Yao'],
        ['Fund Category',       'Equity Fund'],
        ['Launch Date',         '13 December 2021'],
        ['Unit NAV',            fmt_rm(nta)],
        ['Fund Size',           fmt_rm(aum)],
        ['Units in Circulation',fmt_num(units, 2) + ' units'],
        ['Financial Year End',  '30 November'],
        ['Min. Initial Investment', 'RM 10,000.00'],
        ['Min. Additional',     'RM 1,000.00'],
        ['Benchmark',           'FBMKLCI, Tech Index'],
        ['Annual Mgmt Fee',     '0.10% p.a. of NAV'],
        ['Performance Fee',     '10.0% p.a. of excess return'],
        ['Distribution Policy', 'At least 80% of gross income'],
    ]

    story.append(Paragraph("Fund Details", s['section']))
    fd_rows = [[
        Paragraph(r[0], s['small']),
        Paragraph(r[1], s['bold'])
    ] for r in fund_rows]
    fd_table = Table(fd_rows, colWidths=[45*mm, 115*mm])
    fd_table.setStyle(TableStyle([
        ('GRID', (0,0), (-1,-1), 0.4, BORDER),
        ('ROWBACKGROUNDS', (0,0), (-1,-1), [WHITE, GRAY_LIGHT]),
        ('TOPPADDING', (0,0), (-1,-1), 4),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('LEFTPADDING', (0,0), (-1,-1), 6),
        ('FONTSIZE', (0,0), (-1,-1), 8),
        ('TEXTCOLOR', (0,0), (0,-1), GRAY_MED),
    ]))
    story.append(fd_table)
    story.append(Spacer(1, 4*mm))

    # Performance table
    perf = performance.get('period_returns', {})
    story.append(Paragraph("Portfolio Performance Analysis", s['section']))
    perf_headers = ['', '1M', '3M', '6M', '1Y', '3Y', 'All']
    perf_row = ['Portfolio'] + [
        fmt_pct(perf.get(k)) for k in ['1M','3M','6M','1Y','3Y']
    ] + [fmt_pct(fund_data.get('total_return_pct'))]
    build_data_table(story, s,
        headers=perf_headers,
        rows=[perf_row],
        col_widths=[25*mm, 20*mm, 20*mm, 20*mm, 20*mm, 20*mm, 20*mm])

    # Distribution history
    if distributions:
        story.append(Paragraph("Distribution History", s['section']))
        dist_rows = []
        for d in distributions:
            dist_rows.append([
                d.get('financial_year','—'),
                d.get('title','—'),
                fmt_num(d.get('dps_sen'), 2) + ' sen',
                fmt_pct(d.get('payout_ratio')),
            ])
        build_data_table(story, s,
            headers=['Year', 'Description', 'DPS (sen)', 'Payout Ratio'],
            rows=dist_rows,
            col_widths=[20*mm, 90*mm, 30*mm, 25*mm])

    # Largest holdings
    if holdings:
        story.append(Paragraph("Largest Holdings", s['section']))
        h_rows = [[h.get('instrument','—'),
                   fmt_num(h.get('weight_pct') or h.get('mv_portion',0)*100, 2)+'%']
                  for h in holdings[:10]]
        build_data_table(story, s,
            headers=['Asset Name', 'Percentage'],
            rows=h_rows,
            col_widths=[130*mm, 35*mm])

    # Disclaimer
    story.append(Paragraph("<b>Disclaimer</b>", s['notice_title']))
    story.append(Paragraph(
        "Investment involves significant risk, including the potential loss of principal. Past performance is not "
        "indicative of future results. This portfolio update is intended strictly for family members only; it does "
        "not constitute a financial prospectus and is not open to the general public or external investors.",
        s['notice']))
    story.append(Spacer(1, 3*mm))
    footer_table = Table([[
        Paragraph("Head Office: None", s['small']),
        Paragraph("Line: (60)11-1121 8085   Email: nzy.invest@gmail.com", s['small']),
    ]], colWidths=[50*mm, 115*mm])
    footer_table.setStyle(TableStyle([('ALIGN',(1,0),(1,0),'RIGHT'),('TOPPADDING',(0,0),(-1,-1),0)]))
    story.append(footer_table)

    doc.build(story)
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────
# 2. SUBSCRIPTION STATEMENT
# ─────────────────────────────────────────────────────────────
def generate_subscription(investor: dict, cashflows: list,
                           statement_period: str = "") -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
        leftMargin=15*mm, rightMargin=15*mm,
        topMargin=12*mm, bottomMargin=12*mm)
    s = get_styles()
    story = []

    today = date.today()
    name  = investor.get('name','')
    addr  = '\n'.join(filter(None, [
        investor.get('address_line1'),
        investor.get('address_line2'),
        ' '.join(filter(None, [investor.get('postcode'), investor.get('city')])),
        investor.get('state'),
    ]))

    build_header(story, s,
        title="FUND SUBSCRIPTION STATEMENT",
        issued_date=today.strftime('%d-%m-%Y'),
        statement_type="Transaction Statement",
        statement_period=statement_period or today.strftime('%d/%m/%Y'),
        investor_name=name.upper(),
        investor_address=addr)

    # Investor info
    build_investor_info_table(story, s, [
        ['Account Type',    investor.get('account_type','Nominee Account')],
        ['Account ID',      investor.get('account_id','—')],
        ['Registered Name', investor.get('name','—')],
        ['Settlement Type', 'Banking'],
        ['Phone No.',       investor.get('phone','—')],
        ['Bank Name',       investor.get('bank_name','—')],
        ['Email Address',   investor.get('email','—')],
        ['Bank Account No.',investor.get('bank_account_no','—')],
        ['Nominee Name',    '—'],
        ['Total Days Held', f"{investor.get('days_held',0)} days"],
    ])

    # Principal transactions
    if cashflows:
        rows = []
        for cf in cashflows:
            amt  = float(cf.get('amount', 0))
            rows.append([
                fmt_date(cf.get('date')),
                cf.get('description', 'Fund Subscription'),
                fmt_rm(abs(amt)) if amt != 0 else '—',
                fmt_num(cf.get('nta_at_date'), 4) if cf.get('nta_at_date') else '—',
                fmt_num(cf.get('units'), 4) if cf.get('units') else '—',
                fmt_num(cf.get('avg_cost'), 4) if cf.get('avg_cost') else '—',
            ])

        # Opening + closing rows
        total_units = sum(float(cf.get('units',0)) for cf in cashflows if float(cf.get('amount',0)) > 0)
        avg_cost    = cashflows[-1].get('nta_at_date') if cashflows else None
        full_rows = [
            [fmt_date(cashflows[0].get('date')) if cashflows else '—',
             'Opening', '—', '—', '—', '—'],
            *rows,
            [fmt_date(cashflows[-1].get('date')) if cashflows else '—',
             'Closing',
             fmt_rm(sum(abs(float(cf.get('amount',0))) for cf in cashflows)),
             '—',
             fmt_num(total_units, 4),
             fmt_num(avg_cost, 4) if avg_cost else '—'],
        ]

        build_data_table(story, s,
            title="Principal Transaction",
            headers=['Date','Description','Investment Value','Subscription Price','Unit Balanced','Average Cost'],
            rows=full_rows,
            col_widths=[24*mm, 35*mm, 30*mm, 30*mm, 27*mm, 27*mm])

    build_footer(story, s)
    doc.build(story)
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────
# 3. DIVIDEND PAYMENT STATEMENT
# ─────────────────────────────────────────────────────────────
def generate_dividend_statement(investor: dict,
                                 distributions: list,
                                 financial_year: str) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
        leftMargin=15*mm, rightMargin=15*mm,
        topMargin=12*mm, bottomMargin=12*mm)
    s = get_styles()
    story = []

    today = date.today()
    name  = investor.get('name','')

    build_header(story, s,
        title="DIVIDEND PAYMENT STATEMENT",
        issued_date=today.strftime('%d-%m-%Y'),
        statement_type=financial_year,
        statement_period=distributions[0].get('pmt_date','') if distributions else '',
        investor_name=name.upper(),
        investor_address='\n'.join(filter(None, [
            investor.get('address_line1'),
            investor.get('address_line2'),
            ' '.join(filter(None, [investor.get('postcode'), investor.get('city')])),
            investor.get('state'),
        ])))

    # Investor info
    build_investor_info_table(story, s, [
        ["Investor's Name",  investor.get('name','—')],
        ['Settlement Type', 'Banking'],
        ['Phone Number',     investor.get('phone','—')],
        ['Bank Name',        investor.get('bank_name','—')],
        ['Email Address',    investor.get('email','—')],
        ['Bank Account No.', investor.get('bank_account_no','—')],
    ])

    # Dividend details
    if distributions:
        rows = []
        total_dps = 0
        total_amt = 0
        for d in distributions:
            dps    = float(d.get('dps_sen', 0))
            units  = float(d.get('units_at_ex_date', 0))
            amt    = float(d.get('amount', 0))
            eps    = float(d.get('eps', 0)) if d.get('eps') else None
            dpr    = float(d.get('payout_ratio', 0)) if d.get('payout_ratio') else None
            total_dps += dps
            total_amt += amt
            rows.append([
                fmt_date(d.get('pmt_date') or d.get('ex_date')),
                d.get('title', d.get('dist_type','—')).replace('_',' ').title(),
                fmt_num(units, 4),
                fmt_num(eps, 2) if eps else '—',
                f"{dpr:.1f}%" if dpr else '—',
                fmt_num(dps, 2),
                Paragraph(f"<b>{fmt_rm(amt)}</b>",
                    ParagraphStyle('amt', fontName='Helvetica-Bold',
                        fontSize=8, textColor=BLUE, alignment=TA_RIGHT)),
            ])

        build_data_table(story, s,
            title="Dividend Details",
            headers=['Date','Description','Holding Units','EPS','DPR','DPS','Dividend Amount'],
            rows=rows,
            col_widths=[22*mm, 35*mm, 28*mm, 16*mm, 16*mm, 16*mm, 30*mm],
            total_row=['', 'Total', '', '', '',
                       fmt_num(total_dps, 2),
                       fmt_rm(total_amt)])

        story.append(Paragraph(
            "Notes: EPS: Earning Per Share  |  DPR: Dividend Payout Ratio  |  DPS: Dividend Per Share (sen)",
            s['small']))
        story.append(Spacer(1, 2*mm))

    build_footer(story, s)
    doc.build(story)
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────
# 4. INVESTMENT ACCOUNT STATEMENT (Annual)
# ─────────────────────────────────────────────────────────────
def generate_account_statement(investor: dict, summary: dict,
                                cashflows: list, dist_history: list,
                                statement_period: str,
                                financial_year: str) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
        leftMargin=15*mm, rightMargin=15*mm,
        topMargin=12*mm, bottomMargin=12*mm)
    s = get_styles()
    story = []

    today = date.today()
    name  = investor.get('name','')

    def make_header(page_n):
        build_header(story, s,
            title="INVESTMENT ACCOUNT STATEMENT",
            issued_date=today.strftime('%d-%m-%Y'),
            page_info=f"{page_n} of 2",
            statement_type="Annually",
            statement_period=statement_period,
            investor_name=name.upper(),
            investor_address='\n'.join(filter(None, [
                investor.get('address_line1'),
                investor.get('address_line2'),
                ' '.join(filter(None,[investor.get('postcode'),investor.get('city')])),
                investor.get('state'),
            ])))

    # PAGE 1
    make_header(1)

    # Investor info
    build_investor_info_table(story, s, [
        ['Account Type',     summary.get('account_type','Nominee Account')],
        ['Account ID',       investor.get('account_id','—')],
        ['Registered Name',  name],
        ['Settlement Type',  'Banking'],
        ['Phone No.',        investor.get('phone','—')],
        ['Bank Name',        investor.get('bank_name','—')],
        ['Email Address',    investor.get('email','—')],
        ['Bank Account No.', investor.get('bank_account_no','—')],
        ['Nominee Name',     '—'],
        ['Total Days Held',  f"{summary.get('days_held',0)} days"],
    ])

    # Account summary
    nta         = float(summary.get('current_nta', 0))
    units       = float(summary.get('units', 0))
    total_costs = float(summary.get('total_costs', 0))
    market_val  = float(summary.get('market_value', 0))
    unreal_pl   = float(summary.get('unrealized_pl', 0))
    real_pl     = float(summary.get('realized_pl', 0))
    div_rcvd    = float(summary.get('dividends_received', 0))
    adj         = float(summary.get('adjustment', 0))
    total_pl    = unreal_pl + real_pl + div_rcvd + adj
    total_pct   = (total_pl / abs(total_costs) * 100) if total_costs else 0
    irr         = summary.get('irr')

    story.append(Paragraph("Account Summary", s['section']))
    sum_rows = [
        ['(a)', 'Latest Fund Price', fmt_num(units,4), fmt_num(nta,4), fmt_rm(market_val)],
        ['(b)', 'Subscription Cost', fmt_num(units,4), fmt_num(total_costs/units if units else 0,4),
         f"({fmt_rm(total_costs)})"],
        ['(c)', 'Unrealized Profit & Loss:  (a) + (b)', '', '', fmt_rm(unreal_pl)],
        ['(d)', 'Realized Profit & Loss', '', '', fmt_rm(real_pl) if real_pl else '—'],
        ['(e)', 'Dividend Received', '', '', fmt_rm(div_rcvd) if div_rcvd else '—'],
        ['(f)', f'Adjustment: Initial Capital {fmt_rm(abs(adj))}', '', '',
         f"({fmt_rm(abs(adj))})" if adj < 0 else fmt_rm(adj)],
        ['', Paragraph('<b>Total Profit &amp; Loss:  (c) + (d) + (e) + (f)</b>', s['bold']),
         '', '',
         Paragraph(f'<b>{fmt_rm(total_pl)}</b>',
             ParagraphStyle('tp', fontName='Helvetica-Bold', fontSize=8,
                 textColor=GREEN if total_pl >= 0 else RED, alignment=TA_RIGHT))],
        ['', Paragraph('<b>Total Performance %</b>', s['bold']), '', '',
         Paragraph(f'<b>{fmt_pct(total_pct)}</b>',
             ParagraphStyle('pp', fontName='Helvetica-Bold', fontSize=8,
                 textColor=GREEN if total_pct >= 0 else RED, alignment=TA_RIGHT))],
        ['', Paragraph('<b>Annualised Performance* %</b>', s['bold']), '', '',
         Paragraph(f'<b>{fmt_pct(irr) if irr else "—"}</b>',
             ParagraphStyle('ap', fontName='Helvetica-Bold', fontSize=8,
                 textColor=GREEN if irr and float(irr) >= 0 else RED, alignment=TA_RIGHT))],
    ]

    headers_sum = ['', 'Fields', 'Holding Units', 'Average Price', 'Total Value (RM)']
    build_data_table(story, s, headers=headers_sum, rows=sum_rows,
        col_widths=[8*mm, 75*mm, 28*mm, 28*mm, 28*mm])

    story.append(Paragraph(
        "* Powered by Financial Formulation of Internal Rate of Return (IRR) "
        "&amp; Mathematical Algorithm Newton's method", s['small']))

    build_footer(story, s)

    # PAGE 2
    from reportlab.platypus import PageBreak
    story.append(PageBreak())
    make_header(2)

    # Principal transactions
    if cashflows:
        cf_rows = []
        running_units = 0
        for cf in cashflows:
            amt   = float(cf.get('amount', 0))
            u     = float(cf.get('units', 0))
            running_units += u
            nta_d = cf.get('nta_at_date')
            cf_rows.append([
                fmt_date(cf.get('date')),
                cf.get('description','—'),
                fmt_rm(abs(amt)) + (f"\n@ {fmt_rm(nta_d)}" if nta_d else '') if amt else '—',
                fmt_rm(nta_d) if nta_d else '—',
                fmt_num(abs(u), 4) if u else '—',
                fmt_num(running_units, 4) if running_units else '—',
            ])
        build_data_table(story, s,
            title="Principal Transaction",
            headers=['Date','Description','Cashflow @ Price','Avg. Cost (RM)','Units Issued','Units Balanced'],
            rows=cf_rows,
            col_widths=[22*mm, 40*mm, 35*mm, 25*mm, 25*mm, 25*mm])

    # Distribution history
    if dist_history:
        dh_rows = []
        for d in dist_history:
            dh_rows.append([
                fmt_date(d.get('pmt_date') or d.get('ex_date')),
                d.get('title','—'),
                fmt_num(d.get('dps_sen'),2),
                fmt_num(d.get('units_at_ex_date'),4) if d.get('units_at_ex_date') else '—',
                fmt_rm(d.get('amount')) if d.get('amount') else '—',
                fmt_rm(d.get('amount')) if d.get('amount') else '—',
            ])
        build_data_table(story, s,
            title="Distribution Transaction",
            headers=['Date','Description','DPS','Holding Units','Distribution Amount','Balanced (RM)'],
            rows=dh_rows,
            col_widths=[22*mm, 45*mm, 18*mm, 28*mm, 30*mm, 29*mm])

    build_footer(story, s)
    doc.build(story)
    return buf.getvalue()
