(function () {
  function focusSearchInput() {
    var input = document.querySelector('.search-input');
    if (!input) {
      return;
    }
    input.focus();
    input.select();
  }

  function openSearch() {
    var toggle = document.querySelector('.search__toggle');
    if (!toggle) {
      return;
    }
    var input = document.querySelector('.search-input');
    var hidden = !input || input.tabIndex === -1;
    if (hidden) {
      toggle.click();
      window.setTimeout(focusSearchInput, 70);
      return;
    }
    focusSearchInput();
  }

  function bindCmdK() {
    var hint = document.querySelector('.search-shortcut');
    if (hint) {
      hint.textContent = /Mac|iPhone|iPad/.test(navigator.platform) ? 'Cmd+K' : 'Ctrl+K';
    }

    document.addEventListener('keydown', function (event) {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault();
        openSearch();
      }
    });
  }

  function bindInstallSwitcher() {
    var switcher = document.querySelector('[data-install-switcher]');
    if (!switcher) {
      return;
    }

    var tabs = switcher.querySelectorAll('[data-install-tab]');
    var panels = switcher.querySelectorAll('[data-install-panel]');

    function activate(name) {
      tabs.forEach(function (tab) {
        var active = tab.getAttribute('data-install-tab') === name;
        tab.classList.toggle('active', active);
        tab.setAttribute('aria-selected', active ? 'true' : 'false');
      });

      panels.forEach(function (panel) {
        var active = panel.getAttribute('data-install-panel') === name;
        panel.classList.toggle('active', active);
      });
    }

    tabs.forEach(function (tab) {
      tab.addEventListener('click', function () {
        activate(tab.getAttribute('data-install-tab'));
      });
    });
  }

  document.addEventListener('DOMContentLoaded', function () {
    bindCmdK();
    bindInstallSwitcher();
  });
})();
