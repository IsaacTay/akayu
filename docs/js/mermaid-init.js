// Mermaid configuration with Gruvbox Dark theme
//
// Color Scheme Reference (use in diagrams with style directives):
// - Primary (parallel/fused nodes): #458588 (blue)
// - Accent (results/sinks):         #98971a (green)
// - Warning (split points):         #fb4934 (red) - use as stroke
// - Subgraph background:            #3c3836 (dark gray)
// - Convergence (locks):            #fb4934 (red) - use as stroke
//
// Example usage in mermaid:
//   style NodeName fill:#458588
//   style SplitNode stroke:#fb4934,stroke-width:3px

document.addEventListener('DOMContentLoaded', function() {
    mermaid.initialize({
        startOnLoad: false,
        theme: 'dark',
        securityLevel: 'loose',
        flowchart: {
            useMaxWidth: true,
            htmlLabels: true
        },
        themeVariables: {
            // Gruvbox Dark palette
            primaryColor: '#458588',      // Blue - main nodes
            primaryTextColor: '#ebdbb2',  // Light text
            primaryBorderColor: '#83a598',
            lineColor: '#83a598',         // Aqua - arrows
            secondaryColor: '#b16286',    // Purple
            tertiaryColor: '#282828',     // Background
            background: '#282828',
            mainBkg: '#3c3836',           // Node background
            secondBkg: '#504945',
            border1: '#665c54',
            border2: '#504945',
            arrowheadColor: '#83a598',
            fontFamily: 'monospace',
            fontSize: '14px',
            textColor: '#ebdbb2',
            nodeTextColor: '#ebdbb2'
        }
    });

    // Find mermaid code blocks and convert them for rendering
    // Handles multiple formats:
    // 1. <div class="mermaid"> (superfences)
    // 2. <code class="language-mermaid"> (fenced_code)
    // 3. <div class="codehilite"><pre><code> with mermaid content (codehilite)

    // Handle language-mermaid class
    document.querySelectorAll('code.language-mermaid').forEach(function(codeEl) {
        var pre = codeEl.parentElement;
        var div = document.createElement('div');
        div.className = 'mermaid';
        div.textContent = codeEl.textContent;
        pre.parentElement.replaceChild(div, pre);
    });

    // Handle codehilite wrapped mermaid blocks
    // Look for code blocks that start with mermaid keywords
    var mermaidKeywords = /^(graph|flowchart|sequenceDiagram|classDiagram|stateDiagram|erDiagram|gantt|pie|gitGraph|journey)/;
    document.querySelectorAll('.codehilite pre code, pre code').forEach(function(codeEl) {
        var content = codeEl.textContent.trim();
        if (mermaidKeywords.test(content)) {
            var container = codeEl.closest('.codehilite') || codeEl.parentElement;
            var div = document.createElement('div');
            div.className = 'mermaid';
            div.textContent = content;
            container.parentElement.replaceChild(div, container);
        }
    });

    // Render all mermaid diagrams
    mermaid.run({
        querySelector: '.mermaid'
    });
});
