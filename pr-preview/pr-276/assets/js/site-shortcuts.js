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
        tab.tabIndex = active ? 0 : -1;
      });

      panels.forEach(function (panel) {
        var active = panel.getAttribute('data-install-panel') === name;
        panel.classList.toggle('active', active);
        panel.hidden = !active;
      });
    }

    tabs.forEach(function (tab) {
      tab.addEventListener('click', function () {
        activate(tab.getAttribute('data-install-tab'));
      });

      tab.addEventListener('keydown', function (event) {
        var index = Array.prototype.indexOf.call(tabs, tab);
        if (index < 0) {
          return;
        }

        if (event.key === 'ArrowRight') {
          event.preventDefault();
          tabs[(index + 1) % tabs.length].focus();
          return;
        }

        if (event.key === 'ArrowLeft') {
          event.preventDefault();
          tabs[(index - 1 + tabs.length) % tabs.length].focus();
          return;
        }

        if (event.key === 'Home') {
          event.preventDefault();
          tabs[0].focus();
          return;
        }

        if (event.key === 'End') {
          event.preventDefault();
          tabs[tabs.length - 1].focus();
          return;
        }

        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          activate(tab.getAttribute('data-install-tab'));
        }
      });
    });
  }

  function copyText(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      return navigator.clipboard.writeText(text);
    }

    return new Promise(function (resolve, reject) {
      var rtl = document.documentElement.getAttribute('dir') === 'rtl';
      var textarea = document.createElement('textarea');
      textarea.className = 'clipboard-helper';
      textarea.style[rtl ? 'right' : 'left'] = '-9999px';
      textarea.style.top = String(window.pageYOffset || document.documentElement.scrollTop) + 'px';
      textarea.setAttribute('readonly', '');
      textarea.value = text;
      document.body.appendChild(textarea);
      textarea.select();

      try {
        var copied = document.execCommand('copy');
        textarea.remove();
        if (copied) {
          resolve();
          return;
        }
      } catch (error) {
        textarea.remove();
        reject(error);
        return;
      }

      reject(new Error('copy command was rejected'));
    });
  }

  function bindCodeCopyButtons() {
    var codeBlocks = document.querySelectorAll('#main pre > code, .page__content pre > code');
    codeBlocks.forEach(function (code) {
      var block = code.parentElement;
      if (!block || block.tagName.toLowerCase() !== 'pre') {
        return;
      }

      if (block.querySelector(':scope > .clipboard-copy-button')) {
        return;
      }

      if (block.closest('.no-copy')) {
        return;
      }

      var button = document.createElement('button');
      button.type = 'button';
      button.title = 'Copy to clipboard';
      button.setAttribute('data-tooltip', 'Copy');
      button.className = 'clipboard-copy-button';
      button.innerHTML = '<span class="sr-only">Copy code</span><i class="far fa-fw fa-copy"></i><i class="fas fa-fw fa-check copied"></i>';

      button.addEventListener('click', function () {
        copyText(code.innerText)
          .then(function () {
            button.setAttribute('data-tooltip', 'Copied!');
            button.classList.add('copied');
            window.setTimeout(function () {
              button.classList.remove('copied');
              button.setAttribute('data-tooltip', 'Copy');
            }, 1500);
          })
          .catch(function () {
            button.classList.remove('copied');
            button.setAttribute('data-tooltip', 'Copy failed');
            window.setTimeout(function () {
              button.setAttribute('data-tooltip', 'Copy');
            }, 1200);
          });
      });

      block.prepend(button);
    });
  }

  document.addEventListener('DOMContentLoaded', function () {
    bindCmdK();
    bindInstallSwitcher();
    bindCodeCopyButtons();
  });
})();
