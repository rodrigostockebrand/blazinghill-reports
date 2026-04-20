(function () {
  'use strict';

  /* ─── API Configuration ─── */
  // In production (Railway), API is same-origin. In Perplexity sandbox, use port proxy.
  const _port = '__PORT_8000__';
  const API_BASE = _port.startsWith('__') ? '' : _port;

  /* ─── State ─── */
  const state = {
    account: null,
    token: null,
    selectedPlan: null,
    pendingAuthMode: 'signup',
    firstDemoLogin: false
  };

  const plans = {
    Starter: { name: 'Starter', price: '$149/month', credits: 10, total: '$149' },
    Professional: { name: 'Professional', price: '$249/month', credits: 20, total: '$249' },
    Lifetime: { name: 'Lifetime', price: '$5,000 one-time', credits: 'Unlimited', total: '$5,000' }
  };

  const DEMO_CREDITS = 2;

  /* ─── DOM References ─── */
  const authModal = document.getElementById('authModal');
  const checkoutModal = document.getElementById('checkoutModal');
  const appView = document.getElementById('appView');
  const marketingSite = document.getElementById('marketingSite');
  const topbar = document.getElementById('topbar');
  const authForm = document.getElementById('authForm');
  const deepDiveForm = document.getElementById('deepDiveForm');
  const submissionResult = document.getElementById('submissionResult');
  const heroPreviewVideo = document.getElementById('heroPreviewVideo');

  if (heroPreviewVideo) {
    heroPreviewVideo.playbackRate = 0.5;
  }

  function qs(id) { return document.getElementById(id); }
  function openModal(el) { if (el) el.classList.remove('hidden'); }
  function closeModal(el) { if (el) el.classList.add('hidden'); }

  /* ─── API Helpers ─── */
  async function apiPost(endpoint, body) {
    const headers = { 'Content-Type': 'application/json' };
    if (state.token) headers['Authorization'] = `Bearer ${state.token}`;
    const res = await fetch(`${API_BASE}${endpoint}`, { method: 'POST', headers, body: JSON.stringify(body) });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Request failed');
    return data;
  }

  async function apiGet(endpoint) {
    const headers = {};
    if (state.token) headers['Authorization'] = `Bearer ${state.token}`;
    const res = await fetch(`${API_BASE}${endpoint}`, { headers });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Request failed');
    return data;
  }

  async function apiDelete(endpoint) {
    const headers = {};
    if (state.token) headers['Authorization'] = `Bearer ${state.token}`;
    const res = await fetch(`${API_BASE}${endpoint}`, { method: 'DELETE', headers });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Request failed');
    return data;
  }

  /* ─── Token Persistence ─── */
  // Use localStorage when available (Railway / production), fall back to in-memory (sandbox)
  const _canStore = (() => { try { localStorage.setItem('_t', '1'); localStorage.removeItem('_t'); return true; } catch (e) { return false; } })();

  function saveSession(token) {
    state.token = token;
    if (_canStore) localStorage.setItem('bh_token', token);
  }

  function clearSession() {
    state.token = null;
    state.account = null;
    if (_canStore) localStorage.removeItem('bh_token');
  }

  function loadSession() {
    if (state.token) return state.token;
    if (_canStore) return localStorage.getItem('bh_token') || null;
    return null;
  }

  /* ─── View Management ─── */
  function switchPanel(panelId) {
    document.querySelectorAll('.app-panel').forEach((panel) => panel.classList.remove('active'));
    document.querySelectorAll('.app-nav-btn').forEach((btn) => btn.classList.remove('active'));
    const panel = qs(panelId);
    if (panel) panel.classList.add('active');
    const navBtn = document.querySelector(`.app-nav-btn[data-panel="${panelId}"]`);
    if (navBtn) navBtn.classList.add('active');
  }

  function showMarketingSite() {
    document.body.classList.remove('app-mode');
    marketingSite.classList.remove('hidden');
    appView.classList.add('hidden');
    topbar.classList.remove('hidden');
  }

  function showLoggedInWorkspace() {
    document.body.classList.add('app-mode');
    marketingSite.classList.add('hidden');
    appView.classList.remove('hidden');
    topbar.classList.remove('hidden');
  }

  /* ─── Account UI ─── */
  function getCreditUsage(account) {
    if (!account || account.credits === '∞' || account.plan === 'Lifetime') return 100;
    const numericCredits = Number(account.credits) || 0;
    const numericCapacity = Number(account.creditCapacity) || numericCredits || DEMO_CREDITS;
    return Math.max(0, Math.min(100, (numericCredits / numericCapacity) * 100));
  }

  function buildAccountView(user, firstVisit) {
    const isPaid = user.plan !== 'demo';
    const isLifetime = user.plan === 'Lifetime';
    const credits = isLifetime ? '∞' : user.credits;

    return {
      id: user.id,
      name: user.name,
      email: user.email,
      plan: user.plan,
      planLabel: isPaid ? user.plan : 'Demo account',
      credits: credits,
      creditCapacity: user.creditCapacity,
      creditsLabel: isLifetime ? 'Unlimited access' : `${credits} report credits available`,
      status: isPaid ? 'Paid workspace' : 'Demo workspace',
      trackerMeta: isLifetime
        ? 'Unlimited plan activated. Generate reports without a monthly cap.'
        : `${credits} of ${user.creditCapacity} credits currently available in this workspace.`,
      heroTitle: isPaid ? 'Your report environment is active.' : 'Your report environment is ready.',
      heroCopy: isPaid
        ? 'You have an active paid workspace with full credit tracking, report generation, and sample report access.'
        : 'You have 2 complimentary report credits, full workspace access, and sample report access.',
      greeting: firstVisit ? 'Welcome' : 'Welcome back'
    };
  }

  function hydrateAccountUI() {
    if (!state.account) return;
    const account = state.account;
    qs('userNameDisplay').textContent = account.name;
    qs('workspaceGreeting').textContent = account.greeting;
    qs('currentPlanLabel').textContent = account.planLabel;
    qs('creditsLabel').textContent = account.creditsLabel;
    qs('statPlan').textContent = account.planLabel;
    qs('statCredits').textContent = account.credits;
    qs('accountStatusLabel').textContent = account.status;
    qs('trackerMeta').textContent = account.trackerMeta;
    qs('workspaceHeroTitle').textContent = account.heroTitle;
    qs('workspaceHeroCopy').textContent = account.heroCopy;
    qs('creditNotice').textContent = account.plan === 'Lifetime'
      ? 'Unlimited plan: reports are not credit-limited.'
      : 'Submitting this request will consume 1 report credit.';
    qs('creditsMeterFill').style.width = `${getCreditUsage(account)}%`;
    qs('addCreditsTopBtn').textContent = account.plan === 'Lifetime' ? 'Manage plan' : 'Add more credits';
    qs('addCreditsSidebarBtn').textContent = account.plan === 'Lifetime' ? 'Manage plan' : 'Add more credits';

    // Load user's reports
    loadReports();
  }

  function enterApp(account, panelId = 'dashboardPanel') {
    state.account = account;
    hydrateAccountUI();
    showLoggedInWorkspace();
    switchPanel(panelId);
    closeModal(authModal);
    closeModal(checkoutModal);
    appView.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  /* ─── Reports ─── */
  async function loadReports() {
    try {
      const data = await apiGet('/api/reports');
      renderReportsList(data.reports || []);
    } catch (e) {
      console.warn('Could not load reports:', e);
    }
  }

  function renderReportsList(reports) {
    const container = qs('reportsListContainer');
    if (!container) return;

    if (reports.length === 0) {
      container.innerHTML = '<p class="tiny-note">No reports generated yet. Use the "New Report" tab to create your first report.</p>';
      return;
    }

    container.innerHTML = reports.map(r => `
      <div class="report-row" id="report-row-${r.id}">
        <div class="report-row-info">
          <strong>${escapeHtml(r.brand_name)}</strong>
          <span class="report-domain">${escapeHtml(r.domain)}</span>
        </div>
        <div class="report-row-meta">
          <span class="report-market">${escapeHtml(r.market)}</span>
          <span class="report-status status-${r.status}">${r.status === 'completed' ? '✓ Ready' : r.status === 'failed' ? '✗ Failed' : '⏳ Generating...'}</span>
        </div>
        <div class="report-row-actions">
          ${r.status === 'completed' && r.report_url ? `<a href="${r.report_url}" target="_blank" class="secondary-btn small">View report</a>` : ''}
          ${r.status === 'failed' ? `<button class="remove-report-btn" data-report-id="${r.id}" title="Remove failed report">✕ Remove</button>` : ''}
          <span class="report-date">${new Date(r.created_at).toLocaleDateString()}</span>
        </div>
      </div>
    `).join('');

    // Attach remove handlers for failed reports
    container.querySelectorAll('.remove-report-btn').forEach(btn => {
      btn.addEventListener('click', async () => {
        const reportId = btn.dataset.reportId;
        btn.disabled = true;
        btn.textContent = 'Removing...';
        try {
          await apiDelete(`/api/reports/${reportId}`);
          const row = qs(`report-row-${reportId}`);
          if (row) {
            row.style.transition = 'opacity 0.3s ease, transform 0.3s ease';
            row.style.opacity = '0';
            row.style.transform = 'translateX(20px)';
            setTimeout(() => {
              row.remove();
              // If no reports left, show empty message
              if (!container.querySelector('.report-row')) {
                container.innerHTML = '<p class="tiny-note">No reports generated yet. Use the "New Report" tab to create your first report.</p>';
              }
            }, 300);
          }
        } catch (e) {
          btn.disabled = false;
          btn.textContent = '✕ Remove';
          alert(e.message || 'Could not remove report.');
        }
      });
    });
  }

  function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
  }

  /* ─── Auth Handlers ─── */
  function openAuth(mode) {
    state.pendingAuthMode = mode || 'signup';
    const nameGroup = qs('authNameGroup');
    if (state.pendingAuthMode === 'login') {
      qs('authModalEyebrow').textContent = 'Account access';
      qs('authModalTitle').textContent = 'Log into your workspace';
      qs('authModalSub').textContent = 'Access your report environment, generated reports, and billing.';
      qs('authPerk').textContent = 'Returning users can access their workspace, view reports, and manage credits.';
      qs('authSubmitBtn').textContent = 'Log in';
      if (nameGroup) nameGroup.style.display = 'none';
    } else {
      qs('authModalEyebrow').textContent = 'Demo registration';
      qs('authModalTitle').textContent = 'Create your demo account';
      qs('authModalSub').textContent = 'Register to access your workspace and start generating reports immediately.';
      qs('authPerk').textContent = 'Demo accounts receive 2 complimentary report credits — no credit card required.';
      qs('authSubmitBtn').textContent = 'Continue';
      if (nameGroup) nameGroup.style.display = '';
    }
    openModal(authModal);
  }

  /* ─── Pricing → Stripe Payment Links ─── */
  // Stripe Payment Links — direct checkout without requiring login
  const STRIPE_LINKS = {
    Starter: 'https://buy.stripe.com/4gM00l1GHaXK3oreky53O00',
    Professional: 'https://buy.stripe.com/aFadRbadd8PCbUXfoC53O01',
    Lifetime: 'https://buy.stripe.com/8x25kFgBB4zm4svb8m53O02'
  };

  async function goToCheckout(planName) {
    const baseLink = STRIPE_LINKS[planName];
    if (!baseLink) {
      alert('Invalid plan. Please try again.');
      return;
    }

    // If user is logged in, append client_reference_id for automatic credit provisioning
    if (state.token) {
      try {
        const data = await apiGet(`/api/credits/checkout-link/${planName}`);
        window.open(data.url, '_blank');
        return;
      } catch (e) {
        // Fall through to direct link if API fails
        console.warn('Checkout API failed, using direct Stripe link:', e);
      }
    }

    // Not logged in or API failed — go directly to Stripe checkout
    window.open(baseLink, '_blank');
  }

  function promptUpgrade() {
    switchPanel('pricingPanel');
    appView.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  /* ─── Event Listeners ─── */

  // Auth button triggers
  document.querySelectorAll('[data-open-auth]').forEach((btn) => {
    btn.addEventListener('click', () => {
      const mode = btn.dataset.openAuth || 'signup';
      state.selectedPlan = null;
      openAuth(mode);
    });
  });

  // Pricing checkout triggers → redirect to Stripe
  document.querySelectorAll('[data-open-checkout="true"]').forEach((btn) => {
    btn.addEventListener('click', () => {
      const plan = btn.dataset.plan || 'Professional';
      goToCheckout(plan);
    });
  });

  // Modal close buttons
  document.querySelectorAll('[data-close-modal]').forEach((btn) => {
    btn.addEventListener('click', () => closeModal(qs(btn.dataset.closeModal)));
  });

  // Modal backdrop click
  document.querySelectorAll('.modal-backdrop').forEach((backdrop) => {
    backdrop.addEventListener('click', (event) => {
      if (event.target === backdrop) closeModal(backdrop);
    });
  });

  // Auth form submit (register / login)
  authForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    const name = qs('authName').value.trim() || 'Operator';
    const email = qs('authEmail').value.trim() || 'user@fund.com';
    const password = qs('authPassword').value.trim();
    if (!password) return;

    const submitBtn = qs('authSubmitBtn');
    submitBtn.disabled = true;
    submitBtn.textContent = 'Please wait...';

    try {
      let data;
      if (state.pendingAuthMode === 'login') {
        data = await apiPost('/api/auth/login', { email, password });
      } else {
        data = await apiPost('/api/auth/register', { name, email, password });
      }

      saveSession(data.token);
      const account = buildAccountView(data.user, data.firstVisit);
      enterApp(account, 'dashboardPanel');

      // If user was trying to buy a plan, redirect to checkout after login
      if (state.selectedPlan) {
        setTimeout(() => goToCheckout(state.selectedPlan), 500);
        state.selectedPlan = null;
      }
    } catch (e) {
      alert(e.message || 'Authentication failed. Please try again.');
    } finally {
      submitBtn.disabled = false;
      submitBtn.textContent = state.pendingAuthMode === 'login' ? 'Log in' : 'Continue';
    }
  });

  // Demo account button → open signup
  qs('demoAccountBtn').addEventListener('click', () => {
    state.selectedPlan = null;
    openAuth('signup');
  });

  qs('alreadyPaidBtn').addEventListener('click', () => {
    state.selectedPlan = null;
    openAuth('login');
  });

  // Nav buttons
  document.querySelectorAll('.app-nav-btn').forEach((btn) => {
    btn.addEventListener('click', () => switchPanel(btn.dataset.panel));
  });

  document.querySelectorAll('[data-switch-panel]').forEach((btn) => {
    btn.addEventListener('click', () => {
      showLoggedInWorkspace();
      switchPanel(btn.dataset.switchPanel);
      appView.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
  });

  // Sample report triggers
  const SAMPLE_REPORT_URL = './sample-report/index.html';
  function openSampleReport() { window.open(SAMPLE_REPORT_URL, '_blank'); }

  qs('viewSampleHero').addEventListener('click', openSampleReport);
  qs('viewSampleHeroCopy').addEventListener('click', openSampleReport);

  const previewImageTrigger = qs('previewImageTrigger');
  if (previewImageTrigger) {
    previewImageTrigger.addEventListener('click', openSampleReport);
    previewImageTrigger.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); openSampleReport(); }
    });
  }

  const viewSampleHighlights = qs('viewSampleHighlights');
  if (viewSampleHighlights) {
    viewSampleHighlights.addEventListener('click', openSampleReport);
  }

  // Clear old submission result when user starts a new report
  ['brandInput', 'domainInput', 'notesInput'].forEach((id) => {
    const el = qs(id);
    if (el) el.addEventListener('focus', () => {
      submissionResult.classList.add('hidden');
      submissionResult.innerHTML = '';
    });
  });

  // Report creation form
  deepDiveForm.addEventListener('submit', async (event) => {
    event.preventDefault();

    // Clear any previous submission result
    submissionResult.classList.add('hidden');
    submissionResult.innerHTML = '';

    const brand = qs('brandInput').value.trim();
    let domain = qs('domainInput').value.trim();
    const market = qs('marketInput').value;
    const lens = qs('lensInput').value;
    const notes = qs('notesInput').value.trim();

    if (!brand || !domain || !market) {
      alert('Please fill in the brand name, domain, and market.');
      return;
    }

    // Clean domain: strip protocol, www, and trailing slashes
    domain = domain.replace(/^https?:\/\//, '').replace(/^www\./, '').replace(/\/+$/, '');

    const submitBtn = deepDiveForm.querySelector('button[type="submit"]');
    submitBtn.disabled = true;
    submitBtn.textContent = 'Creating report...';

    try {
      const data = await apiPost('/api/reports', {
        brandName: brand,
        domain: domain,
        market: market,
        analysisLens: lens,
        notes: notes
      });

      submissionResult.classList.remove('hidden');
      submissionResult.innerHTML = `
        <strong>Report request submitted.</strong><br>
        <span><strong>Brand:</strong> ${escapeHtml(brand)}</span><br>
        <span><strong>Domain:</strong> ${escapeHtml(domain)}</span><br>
        <span><strong>Market:</strong> ${escapeHtml(market)}</span><br>
        <span><strong>Lens:</strong> ${escapeHtml(lens)}</span><br><br>
        Your report is being generated. Report ID: <code>${data.report.id}</code><br>
        Credits remaining: <strong>${data.creditsRemaining === 'unlimited' ? '∞' : data.creditsRemaining}</strong><br><br>
        <em>You will see your report in the "My Reports" section once it's ready.</em>
      `;

      // Reset form fields for next report
      qs('brandInput').value = '';
      qs('domainInput').value = '';
      qs('notesInput').value = '';

      // Refresh account data
      refreshUser();

      // Poll for report completion
      pollReportStatus(data.report.id);

    } catch (e) {
      alert(e.message || 'Failed to create report. Please try again.');
    } finally {
      submitBtn.disabled = false;
      submitBtn.textContent = 'Create report';
    }
  });

  function pollReportStatus(reportId) {
    let attempts = 0;
    const interval = setInterval(async () => {
      attempts++;
      try {
        const data = await apiGet(`/api/reports/${reportId}`);
        if (data.report.status === 'completed') {
          clearInterval(interval);
          loadReports();
          submissionResult.innerHTML += '<br><strong style="color:#16a34a;">✓ Report is ready! Click "My Reports" to view it.</strong>';
        } else if (data.report.status === 'failed') {
          clearInterval(interval);
          loadReports();
          const notes = data.report.notes || 'Unknown error';
          submissionResult.innerHTML += `<br><strong style="color:#dc2626;">✗ Report generation failed.</strong><br><span style="font-size:0.85em;color:#666;">${notes.slice(0, 200)}</span>`;
        }
      } catch (e) { /* ignore */ }
      if (attempts > 180) clearInterval(interval); // Stop after 30 minutes
    }, 10000);
  }

  async function refreshUser() {
    try {
      const data = await apiGet('/api/auth/me');
      const account = buildAccountView(data.user, false);
      state.account = account;
      hydrateAccountUI();
    } catch (e) {
      console.warn('Could not refresh user:', e);
    }
  }

  // Add credits / upgrade buttons
  ['addCreditsTopBtn', 'addCreditsSidebarBtn', 'upgradeWorkspaceBtn'].forEach((id) => {
    const button = qs(id);
    if (!button) return;
    button.addEventListener('click', promptUpgrade);
  });

  // Logout
  qs('logoutBtn').addEventListener('click', () => {
    clearSession();
    state.selectedPlan = null;
    submissionResult.classList.add('hidden');
    showMarketingSite();
    window.scrollTo({ top: 0, behavior: 'smooth' });
  });

  // Escape to close modals
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
      closeModal(authModal);
      closeModal(checkoutModal);
    }
  });

  /* ─── Init ─── */
  async function init() {
    const savedToken = loadSession();
    if (savedToken) {
      state.token = savedToken;
      try {
        const data = await apiGet('/api/auth/me');
        const account = buildAccountView(data.user, false);
        enterApp(account, 'dashboardPanel');
        return;
      } catch (e) {
        clearSession();
      }
    }
    showMarketingSite();
  }

  // Check if returning from Stripe payment (URL has ?payment=success)
  const urlParams = new URLSearchParams(window.location.search);
  if (urlParams.get('payment') === 'success') {
    // Remove the query param and refresh user data
    window.history.replaceState({}, document.title, window.location.pathname);
    setTimeout(() => refreshUser(), 1000);
  }

  init();
})();
