/* ============================================================
   ZY-Invest Mobile JS  —  shared utilities
   ============================================================ */

const API_BASE = 'https://zy-invest-api.onrender.com';

/* ── API ─────────────────────────────────────────────────── */
async function mFetch(path, options = {}) {
  const token = localStorage.getItem('zy_token');
  const headers = { 'Content-Type': 'application/json', ...options.headers };
  if (token) headers['Authorization'] = `Bearer ${token}`;
  const res = await fetch(`${API_BASE}${path}`, { ...options, headers });
  if (res.status === 401) { mLogout(); throw new Error('Session expired'); }
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.detail || `HTTP ${res.status}`);
  }
  return res.json();
}
const mApi = {
  get:    p      => mFetch(p),
  post:   (p, b) => mFetch(p, { method:'POST',   body: JSON.stringify(b) }),
  put:    (p, b) => mFetch(p, { method:'PUT',    body: JSON.stringify(b) }),
  delete: p      => mFetch(p, { method:'DELETE' }),
};

/* ── Auth ────────────────────────────────────────────────── */
function mUser() {
  return {
    token:      localStorage.getItem('zy_token'),
    name:       localStorage.getItem('zy_name'),
    role:       localStorage.getItem('zy_role'),
    investorId: localStorage.getItem('zy_investor_id'),
  };
}
function mLogout() {
  ['zy_token','zy_role','zy_name','zy_investor_id'].forEach(k => localStorage.removeItem(k));
  window.location.href = '../../login.html';
}
function mAuthCheck() {
  if (!localStorage.getItem('zy_token')) {
    window.location.href = '../../login.html'; return false;
  }
  return true;
}

/* ── Menu ────────────────────────────────────────────────── */
const M_NAV = [
  { href:'index.html',         icon:'📊', label:'Dashboard' },
  { href:'account.html',       icon:'💼', label:'Account Summary' },
  { href:'distributions.html', icon:'💰', label:'My Distributions' },
  { href:'performance.html',   icon:'📈', label:'Fund Performance' },
  { href:'statement.html',     icon:'📋', label:'Corporate Results' },
  { href:'analysis.html',      icon:'🔍', label:'Data Analysis' },
  { href:'documents.html',     icon:'📁', label:'Documents' },
  { href:'profile.html',       icon:'👤', label:'Personal Profile' },
  { href:'security.html',      icon:'🔒', label:'Security & Password' },
];

function mInitMenu(activeHref) {
  const u = mUser();
  // Header
  document.querySelector('.m-menu-user-name').textContent = u.name || '—';
  document.querySelector('.m-menu-user-inv').textContent  =
    u.investorId ? 'Member' : 'Guest';
  // Nav items
  const nav = document.querySelector('.m-menu-nav');
  nav.innerHTML = M_NAV.map(item => `
    <a href="${item.href}"
       class="m-nav-item${item.href === activeHref ? ' active' : ''}">
      <span class="m-nav-icon">${item.icon}</span>
      <span>${item.label}</span>
    </a>`).join('');
  // Logout
  document.querySelector('.m-logout-btn').onclick = async () => {
    try { await mApi.post('/api/auth/logout', {}); } catch(e) {}
    mLogout();
  };
}

function mOpenMenu()  { document.getElementById('m-menu').classList.add('open'); }
function mCloseMenu() { document.getElementById('m-menu').classList.remove('open'); }

/* ── Tabs ────────────────────────────────────────────────── */
function mInitTabs(containerId) {
  const container = document.getElementById(containerId);
  const tabs      = container.querySelectorAll('.m-tab');
  const panels    = container.querySelectorAll('.m-tab-panel');
  tabs.forEach((tab, i) => {
    tab.addEventListener('click', () => {
      tabs.forEach(t => t.classList.remove('active'));
      panels.forEach(p => p.classList.remove('active'));
      tab.classList.add('active');
      panels[i].classList.add('active');
    });
  });
}

/* ── Formatting ──────────────────────────────────────────── */
function mFmtRM(v, dp = 2) {
  if (v === null || v === undefined) return '—';
  const n   = Number(v);
  const abs = Math.abs(n);
  const s   = 'RM ' + abs.toLocaleString('en-MY', {
    minimumFractionDigits: dp, maximumFractionDigits: dp });
  return n < 0 ? '(' + s + ')' : s;
}
function mFmtPct(v) {
  if (v === null || v === undefined) return '—';
  const n = Number(v);
  return (n >= 0 ? '+' : '') + n.toFixed(2) + '%';
}
function mFmtUnits(v) {
  if (!v) return '—';
  return Number(v).toLocaleString('en-MY', { minimumFractionDigits: 4 });
}
function mFmtDate(v) {
  if (!v) return '—';
  return new Date(v + 'T00:00:00').toLocaleDateString('en-GB',
    { day:'2-digit', month:'short', year:'numeric' });
}
function mPlClass(v) { return Number(v) >= 0 ? 'pos' : 'neg'; }
function mEsc(s) {
  return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;')
    .replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
