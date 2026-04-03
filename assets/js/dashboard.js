/* ============================================================
   ZY-Invest Dashboard JS  v1.2.0
   Shared layout, sidebar, auth guard, data loaders
   ============================================================ */

/* ── Sidebar ─────────────────────────────────────────────────── */
function initSidebar() {
  const sidebar  = document.getElementById('sidebar');
  const main     = document.getElementById('dash-main');
  const toggle   = document.getElementById('sidebar-toggle');
  const mobileBtn = document.getElementById('mobile-menu-btn');
  const overlay  = document.getElementById('sidebar-overlay');

  const collapsed = localStorage.getItem('sidebar_collapsed') === 'true';
  if (collapsed) {
    sidebar.classList.add('collapsed');
    main.classList.add('expanded');
  }

  toggle?.addEventListener('click', () => {
    const isCollapsed = sidebar.classList.toggle('collapsed');
    main.classList.toggle('expanded', isCollapsed);
    localStorage.setItem('sidebar_collapsed', isCollapsed);
  });

  mobileBtn?.addEventListener('click', () => {
    sidebar.classList.toggle('mobile-open');
    overlay?.classList.toggle('active');
  });

  overlay?.addEventListener('click', () => {
    sidebar.classList.remove('mobile-open');
    overlay.classList.remove('active');
  });
}

/* ── Render sidebar HTML ─────────────────────────────────────── */
function renderSidebar(activePage) {
  const user = authUser();
  const isAdmin = user.role === 'admin';
  const base = getBasePath();

  const memberNav = `
    <div class="nav-section">
      <div class="nav-section-label">Overview</div>
      <a href="${base}dashboard/index.html" class="nav-item ${activePage==='dashboard'?'active':''}" data-tooltip="Dashboard">
        <span class="nav-icon">📊</span><span class="nav-label">Dashboard</span>
      </a>
    </div>
    <div class="nav-section">
      <div class="nav-section-label">My Account</div>
      <a href="${base}dashboard/account-summary.html" class="nav-item ${activePage==='account-summary'?'active':''}" data-tooltip="Account Summary">
        <span class="nav-icon">💼</span><span class="nav-label">Account Summary</span>
      </a>
      <a href="${base}dashboard/distributions.html" class="nav-item ${activePage==='distributions'?'active':''}" data-tooltip="My Distributions">
        <span class="nav-icon">💎</span><span class="nav-label">My Distributions</span>
      </a>
      <a href="${base}dashboard/profile.html" class="nav-item ${activePage==='profile'?'active':''}" data-tooltip="Personal Profile">
        <span class="nav-icon">👤</span><span class="nav-label">Personal Profile</span>
      </a>
      <a href="${base}dashboard/security.html" class="nav-item ${activePage==='security'?'active':''}" data-tooltip="Security">
        <span class="nav-icon">🔒</span><span class="nav-label">Security &amp; Password</span>
      </a>
    </div>
    <div class="nav-section">
      <div class="nav-section-label">Fund</div>
      <a href="${base}dashboard/performance.html" class="nav-item ${activePage==='performance'?'active':''}" data-tooltip="Fund Performance">
        <span class="nav-icon">📈</span><span class="nav-label">Fund Performance</span>
      </a>
      <a href="${base}dashboard/statement.html" class="nav-item ${activePage==='statement'?'active':''}" data-tooltip="Corporate Results">
        <span class="nav-icon">📋</span><span class="nav-label">Corporate Results</span>
      </a>
      <a href="${base}dashboard/analysis.html" class="nav-item ${activePage==='analysis'?'active':''}" data-tooltip="Data Analysis">
        <span class="nav-icon">🔍</span><span class="nav-label">Data Analysis</span>
      </a>
      <a href="${base}dashboard/documents.html" class="nav-item ${activePage==='documents'?'active':''}" data-tooltip="Documents">
        <span class="nav-icon">📁</span><span class="nav-label">Documents</span>
      </a>
    </div>
  `;

  const adminNav = isAdmin ? `
    <div class="nav-section">
      <div class="nav-section-label">Admin</div>
      <a href="${base}dashboard/admin/index.html" class="nav-item ${activePage==='admin-dashboard'?'active':''}" data-tooltip="Admin Dashboard">
        <span class="nav-icon">🏠</span><span class="nav-label">Admin Dashboard</span>
      </a>
      <a href="${base}dashboard/admin/investors.html" class="nav-item ${activePage==='admin-investors'?'active':''}" data-tooltip="Account Management">
        <span class="nav-icon">👥</span><span class="nav-label">Account Management</span>
      </a>
      <a href="${base}dashboard/admin/principal.html" class="nav-item ${activePage==='admin-principal'?'active':''}" data-tooltip="Principal Cashflow">
        <span class="nav-icon">💵</span><span class="nav-label">Principal Cashflow</span>
      </a>
    </div>
    <div class="nav-section">
      <div class="nav-section-label">Fund Input</div>
      <a href="${base}dashboard/admin/transactions.html" class="nav-item ${activePage==='admin-transactions'?'active':''}" data-tooltip="Trade Transactions">
        <span class="nav-icon">⇄</span><span class="nav-label">Trade Transactions</span>
      </a>
      <a href="${base}dashboard/admin/holdings.html" class="nav-item ${activePage==='admin-holdings'?'active':''}" data-tooltip="Holdings">
        <span class="nav-icon">📦</span><span class="nav-label">Holdings</span>
      </a>
      <a href="${base}dashboard/admin/settlement.html" class="nav-item ${activePage==='admin-settlement'?'active':''}" data-tooltip="Settlement">
        <span class="nav-icon">✓</span><span class="nav-label">Settlement</span>
      </a>
      <a href="${base}dashboard/admin/dividends.html" class="nav-item ${activePage==='admin-dividends'?'active':''}" data-tooltip="Dividends">
        <span class="nav-icon">💰</span><span class="nav-label">Dividends</span>
      </a>
      <a href="${base}dashboard/admin/distributions.html" class="nav-item ${activePage==='admin-distributions'?'active':''}" data-tooltip="Distributions">
        <span class="nav-icon">📤</span><span class="nav-label">Distributions</span>
      </a>
      <a href="${base}dashboard/admin/others.html" class="nav-item ${activePage==='admin-others'?'active':''}" data-tooltip="Others">
        <span class="nav-icon">⚙</span><span class="nav-label">Others</span>
      </a>
    </div>
    <div class="nav-section">
      <div class="nav-section-label">System</div>
      <a href="${base}dashboard/admin/fee-schedule.html" class="nav-item ${activePage==='fee-schedule'?'active':''}" data-tooltip="Fee Schedule">
        <span class="nav-icon">🗓</span><span class="nav-label">Fee Schedule</span>
      </a>
      <a href="${base}dashboard/admin/fee-withdrawal.html" class="nav-item ${activePage==='fee-withdrawal'?'active':''}" data-tooltip="Fee Withdrawal">
        <span class="nav-icon">💸</span><span class="nav-label">Fee Withdrawal</span>
      </a>
      <a href="${base}dashboard/admin/fee-withdrawal.html" class="nav-item ${activePage==='fee-withdrawal'?'active':''}" data-tooltip="Fee Withdrawal">
        <span class="nav-icon">💸</span><span class="nav-label">Fee Withdrawal</span>
      </a>
      <a href="${base}dashboard/admin/price-override.html" class="nav-item ${activePage==='price-override'?'active':''}" data-tooltip="Price Override">
        <span class="nav-icon">💱</span><span class="nav-label">Price Override</span>
      </a>
      <a href="${base}dashboard/admin/documents.html" class="nav-item ${activePage==='admin-documents'?'active':''}" data-tooltip="Document Management">
        <span class="nav-icon">📁</span><span class="nav-label">Document Management</span>
      </a>
      <a href="${base}dashboard/admin/statements.html" class="nav-item ${activePage==='admin-statements'?'active':''}" data-tooltip="Generate Statements">
        <span class="nav-icon">📋</span><span class="nav-label">Generate Statements</span>
      </a>

      </a>
    </div>
  ` : '';

  const initials = (user.name || 'U').split(' ').map(w=>w[0]).join('').toUpperCase().slice(0,2);

  return `
    <div class="sidebar-header">
      <div class="sidebar-logo">
        <img src="${base}assets/img/logo.png" alt="ZY">
        <span>ZY-Invest</span>
      </div>
      <button class="sidebar-toggle" id="sidebar-toggle" title="Collapse sidebar">◀</button>
    </div>
    <nav class="sidebar-nav">
      ${memberNav}
      ${adminNav}
    </nav>
    <div class="sidebar-user">
      <div class="user-card">
        <div class="user-avatar">${initials}</div>
        <div class="user-info">
          <div class="user-name">${user.name || 'Member'}</div>
          <div class="user-role">${user.role || 'member'}</div>
        </div>
      </div>
      <button class="btn-logout" onclick="handleLogout()">⏻ <span class="nav-label">Sign Out</span></button>
    </div>
  `;
}

/* ── Base path helper ─────────────────────────────────────────── */
function getBasePath() {
  const path = window.location.pathname;
  if (path.includes('/dashboard/admin/')) return '../../';
  if (path.includes('/dashboard/')) return '../';
  return '';
}

/* ── Init dashboard page ─────────────────────────────────────── */
function initDashboard(activePage, pageTitle) {
  if (!authRequired()) return;

  const sidebar = document.getElementById('sidebar');
  if (sidebar) sidebar.innerHTML = renderSidebar(activePage);

  const titleEl = document.getElementById('topbar-title');
  if (titleEl) titleEl.textContent = pageTitle || 'Dashboard';

  const dateEl = document.getElementById('topbar-date');
  if (dateEl) dateEl.textContent = new Date().toLocaleDateString('en-MY', {
    weekday: 'short', year: 'numeric', month: 'short', day: 'numeric'
  });

  initSidebar();
}

/* ── Logout ──────────────────────────────────────────────────── */
async function handleLogout() {
  try { await api.post('/api/auth/logout', {}); } catch(e) {}
  authLogout();
}

/* ── Formatters ──────────────────────────────────────────────── */
function fmtRM(val, decimals=2) {
  if (val == null) return '—';
  return 'RM ' + Number(val).toLocaleString('en-MY', {
    minimumFractionDigits: decimals, maximumFractionDigits: decimals
  });
}
function fmtPct(val, decimals=2, showSign=true) {
  if (val == null) return '—';
  const n = Number(val);
  const sign = showSign && n > 0 ? '+' : '';
  return sign + n.toFixed(decimals) + '%';
}
function fmtUnits(val) {
  if (val == null) return '—';
  return Number(val).toLocaleString('en-MY', { maximumFractionDigits: 4 });
}
function fmtDate(val) {
  if (!val) return '—';
  return new Date(val).toLocaleDateString('en-MY', {
    year: 'numeric', month: 'short', day: 'numeric'
  });
}
function fmtNum(val, decimals=0) {
  if (val == null) return '—';
  return Number(val).toLocaleString('en-MY', {
    minimumFractionDigits: decimals, maximumFractionDigits: decimals
  });
}

/* ── Pill helpers ────────────────────────────────────────────── */
function plPill(val) {
  const n = Number(val);
  if (n > 0) return `<span class="pill pill-green">+${fmtRM(n)}</span>`;
  if (n < 0) return `<span class="pill pill-red">${fmtRM(n)}</span>`;
  return `<span class="pill pill-gray">—</span>`;
}
function pctPill(val) {
  const n = Number(val);
  if (n > 0) return `<span class="pill pill-green">${fmtPct(n)}</span>`;
  if (n < 0) return `<span class="pill pill-red">${fmtPct(n)}</span>`;
  return `<span class="pill pill-gray">0.00%</span>`;
}

/* ── Show toast notification ─────────────────────────────────── */
function showToast(msg, type='info') {
  const toast = document.createElement('div');
  toast.style.cssText = `
    position:fixed;bottom:24px;right:24px;z-index:9999;
    padding:12px 20px;border-radius:10px;font-size:0.88rem;font-weight:500;
    box-shadow:0 8px 24px rgba(0,0,0,0.15);
    background:${type==='error'?'#FEF2F2':type==='success'?'#F0FDF4':'#EFF6FF'};
    color:${type==='error'?'#991B1B':type==='success'?'#166534':'#1E40AF'};
    border:1px solid ${type==='error'?'#FECACA':type==='success'?'#BBF7D0':'#BFDBFE'};
    transition:opacity 0.3s;
  `;
  toast.textContent = msg;
  document.body.appendChild(toast);
  setTimeout(() => { toast.style.opacity='0'; setTimeout(()=>toast.remove(), 300); }, 3000);
}
