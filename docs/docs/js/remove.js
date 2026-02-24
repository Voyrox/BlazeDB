(function () {
  function removeFooter() {
    const footers = document.querySelectorAll('footer.col-md-12');
    for (const f of footers) {
      const t = String(f.textContent || '').toLowerCase();
      if (t.includes('documentation built with') && t.includes('mkdocs')) {
        f.remove();
      }
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', removeFooter);
  } else {
    removeFooter();
  }
})();
