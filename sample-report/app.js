/* ═══════════════════════════════════════════════════════
   Meller Brand PE DD Report — App JS
   Sidebar nav, lightbox, hamburger, access gate
   ═══════════════════════════════════════════════════════ */
(function () {
  'use strict';

  /* ── ACCESS GATE ── */
  var gate = document.getElementById('reportGateOverlay');
  var gateForm = document.getElementById('reportGateForm');
  var gateInput = document.getElementById('reportGateInput');
  var gateStatus = document.getElementById('reportGateStatus');
  var reportContent = document.getElementById('reportContent');
  var ACCESS_CODE = 'MELLER2026';

  if (gate && gateForm) {
    gateForm.addEventListener('submit', function (e) {
      e.preventDefault();
      var code = (gateInput.value || '').trim().toUpperCase();
      if (code === ACCESS_CODE) {
        gate.style.display = 'none';
        if (reportContent) reportContent.style.display = '';
        document.body.style.overflow = '';
        initAfterUnlock();
      } else {
        gateStatus.textContent = 'Invalid access code. Please try again.';
        gateStatus.className = 'report-gate-status error';
        gateInput.value = '';
        gateInput.focus();
      }
    });
    // Hide report content until unlocked
    if (reportContent) reportContent.style.display = 'none';
    document.body.style.overflow = 'hidden';
  } else {
    // No gate — init immediately
    initAfterUnlock();
  }

  function initAfterUnlock() {
    initSidebarNav();
    initLightbox();
    initHamburger();
    initScrollSpy();
  }

  /* ── SIDEBAR NAVIGATION ── */
  function initSidebarNav() {
    var links = document.querySelectorAll('.sidebar-nav a');
    links.forEach(function (link) {
      link.addEventListener('click', function (e) {
        e.preventDefault();
        var targetId = this.getAttribute('href').substring(1);
        var target = document.getElementById(targetId);
        if (target) {
          target.scrollIntoView({ behavior: 'smooth', block: 'start' });
          // Close mobile sidebar
          var sidebar = document.getElementById('sidebar');
          var overlay = document.getElementById('overlay');
          var hamburger = document.getElementById('hamburger');
          if (sidebar) sidebar.classList.remove('open');
          if (overlay) overlay.classList.remove('active');
          if (hamburger) hamburger.setAttribute('aria-expanded', 'false');
        }
        // Update active state
        links.forEach(function (l) { l.classList.remove('active'); });
        this.classList.add('active');
      });
    });
  }

  /* ── SCROLL SPY ── */
  function initScrollSpy() {
    var sections = document.querySelectorAll('.section[id]');
    var navLinks = document.querySelectorAll('.sidebar-nav a');
    if (!sections.length || !navLinks.length) return;

    var observer = new IntersectionObserver(function (entries) {
      entries.forEach(function (entry) {
        if (entry.isIntersecting) {
          var id = entry.target.id;
          navLinks.forEach(function (link) {
            link.classList.toggle('active', link.getAttribute('href') === '#' + id);
          });
        }
      });
    }, { rootMargin: '-20% 0px -70% 0px' });

    sections.forEach(function (section) { observer.observe(section); });
  }

  /* ── LIGHTBOX ── */
  function initLightbox() {
    var overlay = document.getElementById('lightbox-overlay');
    var img = document.getElementById('lightbox-img');
    var caption = document.getElementById('lightbox-caption');
    var closeBtn = document.getElementById('lightbox-close');

    if (!overlay || !img) return;

    // Click on any exhibit image to open lightbox
    document.querySelectorAll('.exhibit img').forEach(function (exhibitImg) {
      exhibitImg.addEventListener('click', function () {
        img.src = this.src;
        img.alt = this.alt;
        var fig = this.closest('figure');
        if (fig) {
          var cap = fig.querySelector('figcaption');
          if (cap) caption.innerHTML = cap.innerHTML;
        }
        overlay.classList.add('active');
        document.body.style.overflow = 'hidden';
      });
    });

    function closeLightbox() {
      overlay.classList.remove('active');
      document.body.style.overflow = '';
      img.src = '';
    }

    if (closeBtn) closeBtn.addEventListener('click', closeLightbox);
    overlay.addEventListener('click', function (e) {
      if (e.target === overlay || e.target === img) closeLightbox();
    });
    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape' && overlay.classList.contains('active')) closeLightbox();
    });
  }

  /* ── HAMBURGER (Mobile) ── */
  function initHamburger() {
    var hamburger = document.getElementById('hamburger');
    var sidebar = document.getElementById('sidebar');
    var overlay = document.getElementById('overlay');

    if (!hamburger || !sidebar) return;

    hamburger.addEventListener('click', function () {
      var isOpen = sidebar.classList.toggle('open');
      if (overlay) overlay.classList.toggle('active', isOpen);
      hamburger.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
    });

    if (overlay) {
      overlay.addEventListener('click', function () {
        sidebar.classList.remove('open');
        overlay.classList.remove('active');
        hamburger.setAttribute('aria-expanded', 'false');
      });
    }
  }
})();
