// Mermaid configuration with theme synchronization
document.addEventListener('DOMContentLoaded', function() {
    mermaid.initialize({
        startOnLoad: false,
        theme: 'base',
        securityLevel: 'loose',
        flowchart: {
            useMaxWidth: true,
            htmlLabels: true
        }
    });

    // --- Dynamic Theme Sync ---
    const updateMermaidTheme = () => {
        const html = document.documentElement;
        
        // Shadcn theme stores 'dark' class on document.documentElement
        const isDark = html.classList.contains('dark');

        if (isDark) {
            document.body.setAttribute('data-mermaid-theme', 'dark');
            try {
                // Attempt to re-configure mermaid to force dark theme variables if base theme isn't picking them up
                mermaid.initialize({ 
                    theme: 'base',
                    themeVariables: {
                        darkMode: true,
                        primaryColor: '#32361a',
                        primaryTextColor: '#ebdbb2',
                        primaryBorderColor: '#689d6a',
                        lineColor: '#a89984',
                        background: '#282828'
                    }
                });
            } catch (e) {}
        } else {
            document.body.setAttribute('data-mermaid-theme', 'light');
            try {
                mermaid.initialize({ 
                    theme: 'base',
                    themeVariables: {
                        darkMode: false,
                        primaryColor: '#d1fae5',
                        primaryTextColor: '#064e3b',
                        primaryBorderColor: '#059669',
                        lineColor: '#9ca3af',
                        background: '#ffffff'
                    }
                });
            } catch (e) {}
        }
    };

    // Initial check
    updateMermaidTheme();

    // --- Diagram Preparation ---
    const prepareDiagrams = () => {
        // Handle language-mermaid class (fenced code blocks)
        document.querySelectorAll('code.language-mermaid').forEach(function(codeEl) {
            // Avoid double-processing
            if (codeEl.closest('.mermaid')) return;

            var pre = codeEl.parentElement;
            var div = document.createElement('div');
            div.className = 'mermaid';
            div.setAttribute('data-original-code', codeEl.textContent);
            div.textContent = codeEl.textContent;
            pre.parentElement.replaceChild(div, pre);
        });

        // Handle codehilite wrapped mermaid blocks
        var mermaidKeywords = /^(graph|flowchart|sequenceDiagram|classDiagram|stateDiagram|erDiagram|gantt|pie|gitGraph|journey)/;
        document.querySelectorAll('.codehilite pre code, pre code').forEach(function(codeEl) {
            // Avoid double-processing
            if (codeEl.closest('.mermaid')) return;

            var content = codeEl.textContent.trim();
            if (mermaidKeywords.test(content)) {
                var container = codeEl.closest('.codehilite') || codeEl.parentElement;
                var div = document.createElement('div');
                div.className = 'mermaid';
                div.setAttribute('data-original-code', content);
                div.textContent = content;
                container.parentElement.replaceChild(div, container);
            }
        });
    };

    prepareDiagrams();

    // --- Rendering Logic ---
    const renderDiagrams = () => {
        // Reset all diagrams to their source code
        document.querySelectorAll('.mermaid').forEach(div => {
            const originalCode = div.getAttribute('data-original-code');
            if (originalCode) {
                div.innerHTML = ''; // Clear SVG
                div.textContent = originalCode; // Restore text
                div.removeAttribute('data-processed'); // Clear mermaid flag
            }
        });

        // Re-run mermaid
        mermaid.run({
            querySelector: '.mermaid'
        });
    };

    // Initial render
    renderDiagrams();

    // --- Watcher for Theme Changes ---
    // Re-render when theme changes to ensure CSS variables are picked up by SVGs
    new MutationObserver((mutations) => {
        let shouldRender = false;
        mutations.forEach(m => {
            if (m.attributeName === 'class' && m.target === document.documentElement) {
                shouldRender = true;
            }
        });
        
        if (shouldRender) {
            updateMermaidTheme();
            // Small delay to allow CSS variables to propagate
            setTimeout(renderDiagrams, 100);
        }
    }).observe(document.documentElement, { 
        attributes: true, 
        attributeFilter: ['class'] 
    });
});
