<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>${title ?: 'Nexum Systems'}</title>
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
<body class="theme-shell theme-docs">
<div class="shell">
    ${partial('sidebar')}
    <main class="page-frame">
        <section class="hero-panel hero-panel--systems">
            <div class="hero-panel__grid">
                <div>
                    <p class="eyebrow">${eyebrow ?: 'Systems Overview'}</p>
                    <h1>${title}</h1>
                    <p class="hero-copy">${lead ?: description ?: ''}</p>
                </div>
                <div class="hero-metric">
                    <span class="hero-metric__label">${metricLabel ?: 'Focus'}</span>
                    <strong>${metricValue ?: 'Throughput + clarity'}</strong>
                    <p>${metricCopy ?: 'Design choices stay explicit so the storage path can remain fast, inspectable, and predictable.'}</p>
                </div>
            </div>
        </section>
        <section class="content-panel prose prose--docs">
${content}
        </section>
    </main>
</div>
</body>
</html>
