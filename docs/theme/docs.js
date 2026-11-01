// Postino documentation: navigation drawer, code tabs and copy buttons, the
// "On this page" highlight, and search.
(function () {
  "use strict";

  var root = document.body.dataset.root || "";
  var store = {
    get: function (key) {
      try {
        return localStorage.getItem(key);
      } catch (e) {
        return null;
      }
    },
    set: function (key, value) {
      try {
        localStorage.setItem(key, value);
      } catch (e) {}
    },
  };

  // Navigation drawer on small screens

  var menu = document.querySelector(".menu");
  function setNav(open) {
    document.body.classList.toggle("nav-open", open);
    menu.setAttribute("aria-expanded", String(open));
    menu.setAttribute(
      "aria-label",
      open ? "Hide navigation" : "Show navigation",
    );
  }
  menu.addEventListener("click", function () {
    setNav(!document.body.classList.contains("nav-open"));
  });
  document.addEventListener("click", function (event) {
    if (
      document.body.classList.contains("nav-open") &&
      !event.target.closest(".sidebar") &&
      !event.target.closest(".menu")
    ) {
      setNav(false);
    }
  });

  // Code tabs: choosing a tab chooses it in every group on the page.

  function selectTab(label, save) {
    var groups = document.querySelectorAll(".tabs");
    groups.forEach(function (group) {
      var buttons = group.querySelectorAll('[role="tab"]');
      var has = Array.prototype.some.call(buttons, function (b) {
        return b.dataset.tab === label;
      });
      if (!has) return;
      buttons.forEach(function (b) {
        var on = b.dataset.tab === label;
        b.setAttribute("aria-selected", String(on));
        b.tabIndex = on ? 0 : -1;
      });
      group.querySelectorAll('[role="tabpanel"]').forEach(function (panel) {
        panel.hidden = panel.dataset.tab !== label;
      });
    });
    if (save) store.set("postino-tab", label);
  }

  document.querySelectorAll(".tabs").forEach(function (group) {
    var buttons = Array.prototype.slice.call(
      group.querySelectorAll('[role="tab"]'),
    );
    buttons.forEach(function (button, i) {
      button.addEventListener("click", function () {
        var top = button.getBoundingClientRect().top;
        selectTab(button.dataset.tab, true);
        // Keep the clicked tab in place while other groups change height.
        window.scrollBy(0, button.getBoundingClientRect().top - top);
      });
      button.addEventListener("keydown", function (event) {
        var step =
          event.key === "ArrowRight" ? 1 : event.key === "ArrowLeft" ? -1 : 0;
        if (!step) return;
        event.preventDefault();
        var next = buttons[(i + step + buttons.length) % buttons.length];
        next.focus();
        next.click();
      });
    });
  });
  var savedTab = store.get("postino-tab");
  if (savedTab) selectTab(savedTab, false);

  // Copy buttons

  document.querySelectorAll(".copy").forEach(function (button) {
    button.addEventListener("click", function () {
      var code = button.parentElement.querySelector("code").textContent;
      function done(text) {
        button.textContent = text;
        button.classList.add("done");
        setTimeout(function () {
          button.textContent = "Copy";
          button.classList.remove("done");
        }, 1600);
      }
      if (navigator.clipboard) {
        navigator.clipboard.writeText(code).then(
          function () {
            done("Copied");
          },
          function () {
            done("Press Ctrl+C");
          },
        );
      } else {
        done("Press Ctrl+C");
      }
    });
  });

  // "On this page": highlight the section being read.

  var tocLinks = document.querySelectorAll(".toc a");
  if (tocLinks.length && "IntersectionObserver" in window) {
    var byId = {};
    tocLinks.forEach(function (a) {
      byId[decodeURIComponent(a.hash.slice(1))] = a;
    });
    var headings = Array.prototype.filter.call(
      document.querySelectorAll(".prose h2[id], .prose h3[id]"),
      function (h) {
        return byId[h.id];
      },
    );
    var visible = new Set();
    var observer = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (entry.isIntersecting) visible.add(entry.target);
          else visible.delete(entry.target);
        });
        var current = null;
        for (var i = 0; i < headings.length; i++) {
          if (visible.has(headings[i])) {
            current = headings[i];
            break;
          }
          if (headings[i].getBoundingClientRect().top < 120)
            current = headings[i];
        }
        tocLinks.forEach(function (a) {
          a.classList.remove("active");
        });
        if (current) byId[current.id].classList.add("active");
      },
      { rootMargin: "-64px 0px -55% 0px" },
    );
    headings.forEach(function (h) {
      observer.observe(h);
    });
  }

  // Search

  var input = document.getElementById("search");
  var results = document.getElementById("search-results");
  var active = -1;

  function escapeHtml(text) {
    return text.replace(/[&<>"]/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c];
    });
  }

  function terms(query) {
    return query
      .toLowerCase()
      .split(/[\s,]+/)
      .filter(Boolean);
  }

  function score(entry, words) {
    var page = entry.p.toLowerCase();
    var heading = (entry.h || "").toLowerCase();
    var text = entry.t.toLowerCase();
    var total = 0;
    for (var i = 0; i < words.length; i++) {
      var w = words[i];
      var s = 0;
      if (heading === w) s += 12;
      else if (heading.indexOf(w) === 0) s += 8;
      else if (heading.indexOf(w) >= 0) s += 6;
      if (page.indexOf(w) >= 0) s += 3;
      var re = new RegExp(
        "(^|[^a-z0-9])" + w.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"),
      );
      if (re.test(text)) s += 2;
      else if (text.indexOf(w) >= 0) s += 1;
      if (!s) return 0;
      total += s;
    }
    return total;
  }

  function snippet(text, words) {
    var lower = text.toLowerCase();
    var at = -1;
    for (var i = 0; i < words.length && at < 0; i++)
      at = lower.indexOf(words[i]);
    var start = Math.max(0, at - 40);
    var part =
      (start ? "…" : "") +
      text.slice(start, start + 150) +
      (text.length > start + 150 ? "…" : "");
    var html = escapeHtml(part);
    words.forEach(function (w) {
      var pattern = escapeHtml(w).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      html = html.replace(
        new RegExp("(" + pattern + ")", "gi"),
        "<mark>$1</mark>",
      );
    });
    return html;
  }

  function open(show) {
    results.hidden = !show;
    input.setAttribute("aria-expanded", String(show));
  }

  function render() {
    var words = terms(input.value);
    active = -1;
    if (!words.length) {
      open(false);
      return;
    }
    var index = window.POSTINO_SEARCH || [];
    var hits = [];
    index.forEach(function (entry) {
      var s = score(entry, words);
      if (s) hits.push({ entry: entry, score: s });
    });
    hits.sort(function (a, b) {
      return b.score - a.score;
    });
    hits = hits.slice(0, 8);
    if (!hits.length) {
      results.innerHTML =
        '<p class="empty">Nothing matches “' +
        escapeHtml(input.value.trim()) +
        "”. Try a command name such as XADD, or a word such as lease.</p>";
    } else {
      results.innerHTML = hits
        .map(function (hit, i) {
          var e = hit.entry;
          var where = e.h
            ? escapeHtml(e.h) + " <span>in " + escapeHtml(e.p) + "</span>"
            : escapeHtml(e.p);
          return (
            '<a role="option" id="result-' +
            i +
            '" aria-selected="false" href="' +
            root +
            e.u +
            '"><span class="where">' +
            where +
            '</span><span class="snippet">' +
            snippet(e.t, words) +
            "</span></a>"
          );
        })
        .join("");
    }
    open(true);
  }

  function move(step) {
    var options = results.querySelectorAll("a");
    if (!options.length) return;
    if (active >= 0) options[active].setAttribute("aria-selected", "false");
    active = (active + step + options.length) % options.length;
    options[active].setAttribute("aria-selected", "true");
    options[active].scrollIntoView({ block: "nearest" });
    input.setAttribute("aria-activedescendant", options[active].id);
  }

  input.addEventListener("input", render);
  input.addEventListener("focus", function () {
    if (input.value.trim()) render();
  });
  input.addEventListener("keydown", function (event) {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      move(1);
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      move(-1);
    } else if (event.key === "Enter") {
      var options = results.querySelectorAll("a");
      var target = options[active >= 0 ? active : 0];
      if (target) location.href = target.href;
    } else if (event.key === "Escape") {
      if (input.value) input.value = "";
      open(false);
      input.blur();
    }
  });
  document.addEventListener("click", function (event) {
    if (!event.target.closest(".search")) open(false);
  });
  document.addEventListener("keydown", function (event) {
    var typing = /^(INPUT|TEXTAREA|SELECT)$/.test(
      document.activeElement.tagName,
    );
    if (
      (event.key === "/" && !typing) ||
      (event.key === "k" && (event.metaKey || event.ctrlKey))
    ) {
      event.preventDefault();
      input.focus();
      input.select();
    } else if (event.key === "Escape") {
      setNav(false);
    }
  });
})();
