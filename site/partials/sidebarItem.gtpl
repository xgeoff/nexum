<% def navItem = item ?: [:] %>
<% if (navItem.type == 'directory') { %>
<li class="sidebar__group">
    <% def groupLabel = (navItem.name ?: '').replace('-', ' ') %>
    <span>${groupLabel}</span>
    <ul>
    <% (navItem.children ?: []).each { child -> %>
        ${partial('sidebarItem', [item: child])}
    <% } %>
    </ul>
</li>
<% } else if (navItem.type == 'file') { %>
<% def rawBase = baseUrl ?: '' %>
<% def normalizedBase = rawBase.length() > 1 && rawBase.endsWith('/') ? rawBase[0..-2] : rawBase %>
<% def href = normalizedBase ? "${normalizedBase}/${navItem.path}.html" : "/${navItem.path}.html" %>
<%
def fileLabel = navItem.name == 'index' ? 'Overview' : (navItem.name ?: '')
fileLabel = fileLabel
        .replace('-', ' ')
        .split(/\s+/)
        .collect { it ? it[0].toUpperCase() + it.substring(1) : it }
        .join(' ')
%>
<li class="sidebar__file"><a href="${href}">${fileLabel}</a></li>
<% } %>
