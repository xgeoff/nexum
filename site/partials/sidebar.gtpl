<div class="sidebar">
    <div class="sidebar__brand">
        <a class="sidebar__brand-link" href="${baseUrl ?: '/'}">
            <span class="sidebar__mark"></span>
            <span>
                <strong>Nexum</strong>
                <small>Native database</small>
            </span>
        </a>
    </div>
    <p class="sidebar__tagline">A modern, high-performance database built around one native storage core.</p>
    <nav class="sidebar__nav">
        <p class="sidebar__section-title">Documentation</p>
        <ul>
        <% (navigation ?: []).each { item -> %>
            ${partial('sidebarItem', [item: item])}
        <% } %>
        </ul>
    </nav>
</div>
