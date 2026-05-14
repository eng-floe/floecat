(function () {
  "use strict";

  // Compute the docs parent root so "back to Floecat" works across:
  // /floecat/documentation/ and /floecat/pr-preview/pr-<n>/documentation/
  function computeFloecatHome(pathname) {
    var marker = "/documentation";
    var idx = pathname.indexOf(marker);
    if (idx >= 0) {
      var prefix = pathname.slice(0, idx).replace(/\/+$/, "");
      return (prefix || "") + "/";
    }
    return "/";
  }

  function ensureHeaderHomeLink(homeHref) {
    var headerInner = document.querySelector(".md-header__inner");
    if (!headerInner) return;

    var existing = headerInner.querySelector(".floecat-home-link");
    if (!existing) {
      // Add a compact icon + label pill in the header actions area.
      var option = document.createElement("div");
      option.className = "md-header__option floecat-home-option";

      var link = document.createElement("a");
      link.className = "floecat-home-link";
      link.setAttribute("data-floecat-home-link", "");
      link.title = "Back to Floecat";
      link.setAttribute("aria-label", "Back to Floecat main site");

      var icon = document.createElement("span");
      icon.className = "floecat-home-link__icon";
      icon.setAttribute("aria-hidden", "true");

      var label = document.createElement("span");
      label.className = "floecat-home-link__label";
      label.textContent = "Back to Floecat";

      link.appendChild(icon);
      link.appendChild(label);

      option.appendChild(link);

      var searchButton = headerInner.querySelector("label.md-header__button[for='__search']");
      if (searchButton) {
        headerInner.insertBefore(option, searchButton);
      } else {
        headerInner.appendChild(option);
      }
      existing = link;
    }

    existing.setAttribute("href", homeHref);
  }

  function applyHomeLinks() {
    var homeHref = computeFloecatHome(window.location.pathname);

    document.querySelectorAll("[data-floecat-home-link]").forEach(function (link) {
      link.setAttribute("href", homeHref);
    });

    // Keep the default MkDocs logo link aligned with Floecat home too.
    document
      .querySelectorAll("a.md-header__button.md-logo, a.md-nav__button.md-logo")
      .forEach(function (logoLink) {
        logoLink.setAttribute("href", homeHref);
        logoLink.setAttribute("title", "Back to Floecat");
        logoLink.setAttribute("aria-label", "Back to Floecat main site");
      });

    ensureHeaderHomeLink(homeHref);
  }

  function applyAccessibilityFixes() {
    // MkDocs search dialog misses an explicit accessible name in our setup.
    // Set one at runtime so Lighthouse and screen readers pass.
    var searchDialog = document.querySelector(".md-search[role='dialog']");
    if (searchDialog && !searchDialog.getAttribute("aria-label")) {
      searchDialog.setAttribute("aria-label", "Search documentation");
    }
  }

  function applyEnhancements() {
    applyHomeLinks();
    applyAccessibilityFixes();
  }

  document.addEventListener("DOMContentLoaded", applyEnhancements);
  if (typeof window.document$ !== "undefined" && window.document$) {
    window.document$.subscribe(applyEnhancements);
  }
})();
