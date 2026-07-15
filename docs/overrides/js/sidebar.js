// Preserve the sidebar's position between document navigations without
// animating it into a new position on every page load. The upstream script
// centers the active link with smooth scrolling, which makes static-page
// navigation look like the sidebar is flashing.
const sidebar = document.querySelector('[data-sidebar="content"]');

if (sidebar) {
  const saved = sessionStorage.getItem("sidebar-scroll");
  if (saved !== null) {
    sidebar.scrollTop = Number.parseInt(saved, 10);
  }

  window.addEventListener("pagehide", () => {
    sessionStorage.setItem("sidebar-scroll", String(sidebar.scrollTop));
  });
}
