<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>${title ?: 'Nexum'}</title>
    <meta name="description" content="${description ?: ''}">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=Space+Grotesk:wght@400;500;700&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="css/normalize.css">
    <link rel="stylesheet" href="css/skeleton.css">
    <link rel="stylesheet" href="css/style.css">
    <link rel="icon" type="image/png" href="images/favicon-32.png">
</head>
<body class="theme-shell theme-home">
<div class="shell">
    ${partial('sidebar')}
    <main class="page-frame">
        <section class="hero-panel hero-panel--home">
            <p class="eyebrow">${eyebrow ?: 'Native Multi-Model Engine'}</p>
            <h1>${title ?: 'Nexum'}</h1>
            <p class="hero-copy">${lead ?: description ?: 'A new embedded database focused on disciplined architecture, low-latency storage paths, and one high-performance core shared by graph, relational, and object facades.'}</p>
            <div class="signal-grid">
                <article class="signal-card">
                    <span class="signal-card__label">Engine</span>
                    <strong>Single storage core</strong>
                    <p>Shared record, schema, WAL, and index primitives drive every facade.</p>
                </article>
                <article class="signal-card">
                    <span class="signal-card__label">Access Model</span>
                    <strong>SWMR by design</strong>
                    <p>Committed snapshots stay stable while the writer advances in a private overlay.</p>
                </article>
                <article class="signal-card">
                    <span class="signal-card__label">Interface</span>
                    <strong>Explicit APIs</strong>
                    <p>No magic layers, no runtime persistence tricks, and no ambiguity about ownership.</p>
                </article>
            </div>
        </section>
        <section class="content-panel content-panel--home prose prose--home">
${content}
        </section>
    </main>
</div>
</body>
</html>
