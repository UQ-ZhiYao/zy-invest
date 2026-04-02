/* ============================================================
   ZY-Invest  API + Auth utilities  v1.1.0
   ============================================================ */

const API_BASE = 'https://zy-invest-api.onrender.com';

/* ── HTTP helpers ─────────────────────────────────────────── */
async function apiFetch(path, options = {}) {
  const token = localStorage.getItem('zy_token');
  const headers = { 'Content-Type': 'application/json', ...options.headers };
  if (token) headers['Authorization'] = `Bearer ${token}`;
  const res = await fetch(`${API_BASE}${path}`, { ...options, headers });
  if (res.status === 401) { authLogout(); throw new Error('Session expired — please log in again'); }
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.detail || `HTTP ${res.status}`);
  }
  return res.json();
}

const api = {
  get:    (path)         => apiFetch(path),
  post:   (path, body)   => apiFetch(path, { method: 'POST',   body: JSON.stringify(body) }),
  put:    (path, body)   => apiFetch(path, { method: 'PUT',    body: JSON.stringify(body) }),
  delete: (path)         => apiFetch(path, { method: 'DELETE' }),
};

/* ── Auth helpers ─────────────────────────────────────────── */
function authSave(data) {
  localStorage.setItem('zy_token',       data.access_token);
  localStorage.setItem('zy_role',        data.role);
  localStorage.setItem('zy_name',        data.name);
  localStorage.setItem('zy_investor_id', data.investor_id || '');
}

function authLogout() {
  ['zy_token','zy_role','zy_name','zy_investor_id'].forEach(k => localStorage.removeItem(k));
  window.location.href = '/login.html';
}

function authUser() {
  return {
    token:      localStorage.getItem('zy_token'),
    role:       localStorage.getItem('zy_role'),
    name:       localStorage.getItem('zy_name'),
    investorId: localStorage.getItem('zy_investor_id'),
  };
}

function authRequired() {
  const { token } = authUser();
  if (!token) { window.location.href = '/login.html'; return false; }
  return true;
}

function adminRequired() {
  const { token, role } = authUser();
  if (!token) { window.location.href = '/login.html'; return false; }
  if (role !== 'admin') { window.location.href = '/dashboard/index.html'; return false; }
  return true;
}

/* ── Navbar scroll effect ─────────────────────────────────── */
function initNavbar() {
  const nav = document.querySelector('.navbar');
  if (!nav) return;
  window.addEventListener('scroll', () => {
    nav.classList.toggle('scrolled', window.scrollY > 10);
  });
  // Mobile toggle
  const toggle = document.querySelector('.navbar-toggle');
  const navLinks = document.querySelector('.navbar-nav');
  if (toggle && navLinks) {
    toggle.addEventListener('click', () => navLinks.classList.toggle('open'));
  }
  // Active link
  const path = window.location.pathname;
  document.querySelectorAll('.navbar-nav a').forEach(a => {
    if (a.getAttribute('href') && path.endsWith(a.getAttribute('href').split('/').pop())) {
      a.classList.add('active');
    }
  });
}

/* ── Fund overview (public) ───────────────────────────────── */
async function loadFundOverview() {
  try {
    const data = await api.get('/api/public/fund-overview');
    if (!data) return;
    // Fill any element with data-fund="key"
    document.querySelectorAll('[data-fund]').forEach(el => {
      const key = el.dataset.fund;
      if (data[key] !== undefined) {
        let val = data[key];
        if (key === 'aum') val = 'RM ' + Number(val).toLocaleString('en-MY', {maximumFractionDigits:0});
        if (key === 'current_nta') val = Number(val).toFixed(4);
        if (key === 'total_return_pct') val = '+' + Number(val).toFixed(2) + '%';
        if (key === 'trading_days') val = Number(val).toLocaleString() + '+';
        el.textContent = val;
      }
    });
    // Portfolio snapshot bars
    const snapshot = document.getElementById('portfolio-snapshot');
    if (snapshot && data.portfolio_snapshot) {
      snapshot.innerHTML = data.portfolio_snapshot.map(item => `
        <div class="snapshot-row">
          <div class="snapshot-label">
            <span>${item.asset_class}</span>
            <span class="snapshot-pct">${Number(item.weight_pct).toFixed(1)}%</span>
          </div>
          <div class="snapshot-bar-bg">
            <div class="snapshot-bar" style="width:${item.weight_pct}%"></div>
          </div>
        </div>
      `).join('');
    }
  } catch(e) { console.warn('Fund overview unavailable:', e.message); }
}

document.addEventListener('DOMContentLoaded', initNavbar);
