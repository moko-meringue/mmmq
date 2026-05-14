/* =========================================================
   Code block · 복사 버튼
   ========================================================= */
(function () {
    const COPIED_LABEL = '복사됨';
    const RESET_MS = 1000;
    const CHECK_ICON = '<span class="code-block__copy-icon" aria-hidden="true">' +
        '<svg viewBox="0 0 16 16">' +
        '<path d="M3 8.5l3.5 3.5L13 5" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>' +
        '</svg></span>';

    function legacyCopy(text) {
        const ta = document.createElement('textarea');
        ta.value = text;
        ta.style.position = 'fixed';
        ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select();
        try { document.execCommand('copy'); } catch (e) { /* ignore */ }
        document.body.removeChild(ta);
    }

    document.addEventListener('click', function (event) {
        const button = event.target.closest('.code-block__copy');
        if (!button) return;

        const block = button.closest('.code-block');
        const pre = block && block.querySelector('pre');
        if (!pre) return;

        if (!button.dataset.originalLabel) {
            button.dataset.originalLabel = button.textContent;
        }

        const showCopied = () => {
            button.innerHTML = CHECK_ICON + ' ' + COPIED_LABEL;
            button.classList.add('is-copied');
            clearTimeout(button._copiedTimeout);
            button._copiedTimeout = setTimeout(() => {
                button.textContent = button.dataset.originalLabel;
                button.classList.remove('is-copied');
            }, RESET_MS);
        };

        const text = pre.innerText;
        if (navigator.clipboard && window.isSecureContext) {
            navigator.clipboard.writeText(text).then(showCopied).catch(() => {
                legacyCopy(text);
                showCopied();
            });
        } else {
            legacyCopy(text);
            showCopied();
        }
    });
})();


/* =========================================================
   Sidebar TOC · 자동 생성 + 스크롤 스파이
   ---------------------------------------------------------
   각 페이지의 `<section id="…">` 블록을 스캔해 우측 TOC를 자동 생성.
     - h1/h2를 1단계 (섹션) 항목으로
     - 그 안의 h3를 2단계 (서브섹션) 항목으로 중첩
     - h3에 id가 없으면 자동 생성 (parent.id + '--' + slug)

   data-toc-static 속성이 붙은 TOC는 처리하지 않음 (디자인 시스템 데모 등).

   스크롤 스파이 동작:
     - 읽기 선(`scrollY + READ_LINE`) 통과한 가장 마지막 navigable이 active.
     - h3가 active이면 그 부모 h2도 같이 active 표시 (계층 강조).
     - 페이지 맨 아래(viewportBottom == scrollHeight)에서는 읽기 선이
       마지막 navigable의 top까지 도달할 수 없으므로, viewport에 보이는
       헤딩 중 가장 깊은 것을 active로 fallback.

   클릭 의도 보존(pin):
     - 클릭한 navigable의 desired scrollY(`offsetTop - 24`)가 페이지
       maxScroll보다 커서 캡되는 경우에만 pin 활성화.
     - pin은 anchor scrollY(maxScroll)에서 PIN_RELEASE_DELTA px 이상
       멀어지면 즉시 해제.
   ========================================================= */
(function () {
    const tocs = Array.from(document.querySelectorAll('.docs-toc'))
        .filter(t => !t.dataset.tocStatic);
    if (tocs.length === 0) return;

    for (const toc of tocs) {
        setupToc(toc);
    }

    function setupToc(toc) {
        const ol = toc.querySelector('ol');
        if (!ol) return;

        const grid = toc.closest('.docs-grid, .about-grid, .quickstart-grid');
        const root = grid || document.body;

        const navigables = collectNavigables(root);
        if (navigables.length === 0) return;

        renderNestedToc(ol, navigables);
        setupScrollSpy(toc, navigables);
    }

    // 페이지 안의 navigable 트리 만들기.
    // 반환: [{ id, label, level (2|3), parentId|null, el }]
    function collectNavigables(root) {
        const navs = [];
        const sections = root.querySelectorAll('section[id]');
        const usedIds = new Set();

        for (const section of sections) {
            const heading = findSectionHeading(section);
            if (!heading) continue;

            const sectionId = section.id;
            usedIds.add(sectionId);
            navs.push({
                id: sectionId,
                label: textOf(heading),
                level: 2,
                parentId: null,
                el: section
            });

            const h3s = Array.from(section.querySelectorAll('h3'))
                .filter(h3 => h3.closest('section[id]') === section);

            for (const h3 of h3s) {
                if (!h3.id) {
                    h3.id = makeUniqueId(sectionId, textOf(h3), usedIds);
                }
                usedIds.add(h3.id);
                navs.push({
                    id: h3.id,
                    label: textOf(h3),
                    level: 3,
                    parentId: sectionId,
                    el: h3
                });
            }
        }

        return navs;
    }

    function findSectionHeading(section) {
        const headings = section.querySelectorAll('h1, h2');
        for (const h of headings) {
            if (h.closest('section[id]') === section) return h;
        }
        return null;
    }

    function textOf(el) {
        return (el.textContent || '').trim().replace(/\s+/g, ' ');
    }

    function makeUniqueId(parentId, text, usedIds) {
        const baseSlug = slugify(text) || 'section';
        let candidate = parentId + '--' + baseSlug;
        let n = 2;
        while (usedIds.has(candidate)) {
            candidate = parentId + '--' + baseSlug + '-' + n;
            n++;
        }
        return candidate;
    }

    function slugify(text) {
        return (text || '').trim()
            .replace(/[(){}\[\]<>'"!?,;:.\/\\@#$%^&*+=`~|]/g, '')
            .replace(/\s+/g, '-')
            .replace(/-+/g, '-')
            .replace(/^-+|-+$/g, '')
            .toLowerCase();
    }

    function renderNestedToc(ol, navigables) {
        ol.innerHTML = '';
        let currentParentLi = null;
        let currentParentHead = null;
        let currentParentNav = null;
        let currentSubOl = null;

        for (const nav of navigables) {
            if (nav.level === 2) {
                const li = document.createElement('li');
                const head = document.createElement('div');
                head.className = 'docs-toc__group-head';
                head.appendChild(makeLink(nav));
                li.appendChild(head);
                ol.appendChild(li);
                currentParentLi = li;
                currentParentHead = head;
                currentParentNav = nav;
                currentSubOl = null;
            } else if (nav.level === 3) {
                if (!currentParentLi) continue;
                if (!currentSubOl) {
                    currentSubOl = document.createElement('ol');
                    currentSubOl.className = 'docs-toc__sub';
                    currentSubOl.id = 'docs-toc-sub--' + currentParentNav.id;
                    currentParentLi.appendChild(currentSubOl);
                    currentParentLi.classList.add('docs-toc__group');
                    currentParentLi.setAttribute('data-collapsed', 'true');
                    currentParentHead.appendChild(makeToggleButton(currentParentLi, currentSubOl.id));
                }
                const li = document.createElement('li');
                li.appendChild(makeLink(nav));
                currentSubOl.appendChild(li);
            }
        }
    }

    function makeLink(nav) {
        const a = document.createElement('a');
        a.href = '#' + nav.id;
        a.textContent = nav.label;
        // h2 링크 클릭은 네비게이션만 — 펼침/접기는 화살표 버튼 전담.
        return a;
    }

    function makeToggleButton(li, controlsId) {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'docs-toc__toggle';
        btn.setAttribute('aria-expanded', 'false');
        btn.setAttribute('aria-controls', controlsId);
        btn.setAttribute('aria-label', '하위 섹션 접기/펼치기');
        btn.innerHTML = '<svg class="docs-toc__toggle-icon" aria-hidden="true" viewBox="0 0 12 12">' +
            '<path d="M3 4.5l3 3 3-3" fill="none" stroke="currentColor" stroke-width="1.5" ' +
            'stroke-linecap="round" stroke-linejoin="round"/></svg>';
        btn.addEventListener('click', () => {
            const isCollapsed = li.getAttribute('data-collapsed') === 'true';
            setCollapsed(li, !isCollapsed);
        });
        return btn;
    }

    function setCollapsed(group, collapsed) {
        group.setAttribute('data-collapsed', collapsed ? 'true' : 'false');
        const toggle = group.querySelector(':scope > .docs-toc__group-head > .docs-toc__toggle');
        if (toggle) {
            toggle.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
        }
        // 스크롤 스파이가 active 표시를 다시 계산하도록 알림
        group.dispatchEvent(new CustomEvent('toc:collapse-toggle', { bubbles: true }));
    }

    function setupScrollSpy(toc, navigables) {
        const linkById = new Map();
        const elById = new Map();
        const parentByChildId = new Map();
        for (const nav of navigables) {
            const link = toc.querySelector('a[href="#' + cssEscape(nav.id) + '"]');
            if (link) linkById.set(nav.id, link);
            elById.set(nav.id, nav.el);
            if (nav.parentId) parentByChildId.set(nav.id, nav.parentId);
        }
        if (linkById.size === 0) return;

        const sortedNav = navigables.slice().sort((a, b) => offsetTopOf(a.el) - offsetTopOf(b.el));

        const READ_LINE = 100;
        const BOTTOM_EPSILON = 4;
        const SCROLL_MARGIN = 24;
        const PIN_RELEASE_DELTA = 30;

        let activeId = null;
        let pinnedId = null;
        let pinAnchorY = null;

        // h3가 active인데 그 부모 그룹이 접혀 있으면, 표시는 부모 h2로 대체.
        // (실제 activeId는 그대로 유지 — 펼치는 순간 h3 표시로 자연스럽게 복귀.)
        function effectiveActiveId() {
            if (activeId === null) return null;
            const parentId = parentByChildId.get(activeId);
            if (!parentId) return activeId;
            const parentLink = linkById.get(parentId);
            if (!parentLink) return activeId;
            const group = parentLink.closest('.docs-toc__group');
            if (group && group.getAttribute('data-collapsed') === 'true') {
                return parentId;
            }
            return activeId;
        }

        function applyActive() {
            const targetId = effectiveActiveId();
            for (const link of linkById.values()) {
                link.classList.remove('is-active');
                link.removeAttribute('aria-current');
            }
            if (targetId === null) return;
            const link = linkById.get(targetId);
            if (link) {
                link.classList.add('is-active');
                link.setAttribute('aria-current', 'true');
            }
        }

        function setActive(id) {
            if (id === activeId) return;
            activeId = id;
            applyActive();
        }

        // collapse 토글 이벤트 → 표시 활성 항목 재계산
        toc.addEventListener('toc:collapse-toggle', applyActive);

        function navFromScroll() {
            const viewportTop = window.scrollY;
            const viewportBottom = viewportTop + window.innerHeight;
            const docHeight = document.documentElement.scrollHeight;

            if (viewportBottom >= docHeight - BOTTOM_EPSILON) {
                let deepest = null;
                for (const nav of sortedNav) {
                    const top = offsetTopOf(nav.el);
                    if (top >= viewportTop && top <= viewportBottom) {
                        deepest = nav;
                    }
                }
                return (deepest || sortedNav[sortedNav.length - 1]).id;
            }

            const readLine = viewportTop + READ_LINE;
            if (readLine < offsetTopOf(sortedNav[0].el)) return sortedNav[0].id;
            for (let i = sortedNav.length - 1; i >= 0; i--) {
                if (readLine >= offsetTopOf(sortedNav[i].el)) return sortedNav[i].id;
            }
            return sortedNav[0].id;
        }

        function pinForTarget(id) {
            const el = elById.get(id);
            if (!el) return null;
            const desiredY = offsetTopOf(el) - SCROLL_MARGIN;
            const maxY = document.documentElement.scrollHeight - window.innerHeight;
            return desiredY > maxY ? maxY : null;
        }

        function update() {
            if (pinnedId !== null && pinAnchorY !== null) {
                if (Math.abs(window.scrollY - pinAnchorY) < PIN_RELEASE_DELTA) {
                    setActive(pinnedId);
                    return;
                }
                pinnedId = null;
                pinAnchorY = null;
            }
            setActive(navFromScroll());
        }

        for (const link of linkById.values()) {
            link.addEventListener('click', () => {
                const id = link.getAttribute('href').slice(1);
                const anchor = pinForTarget(id);
                setActive(id);
                if (anchor !== null) {
                    pinnedId = id;
                    pinAnchorY = anchor;
                } else {
                    pinnedId = null;
                    pinAnchorY = null;
                }
            });
        }

        window.addEventListener('scroll', update, { passive: true });
        window.addEventListener('resize', update, { passive: true });

        if (location.hash) {
            const id = location.hash.slice(1);
            if (linkById.has(id)) {
                // 해시가 h3를 가리키면, 그 부모 그룹을 자동으로 펼쳐 둔다 (위치 노출용).
                const parentId = parentByChildId.get(id);
                if (parentId) {
                    const parentLink = linkById.get(parentId);
                    const group = parentLink && parentLink.closest('.docs-toc__group');
                    if (group && group.getAttribute('data-collapsed') === 'true') {
                        setCollapsed(group, false);
                    }
                }
                const anchor = pinForTarget(id);
                if (anchor !== null) {
                    pinnedId = id;
                    pinAnchorY = anchor;
                }
            }
        }
        update();
    }

    // 한 번의 reflow를 피하지는 않지만 코드 일관성을 위해 함수로 묶음
    function offsetTopOf(el) {
        let y = 0;
        let cur = el;
        while (cur) {
            y += cur.offsetTop || 0;
            cur = cur.offsetParent;
        }
        return y;
    }

    // 안전한 CSS 셀렉터 escape (CSS.escape가 없는 환경 대비)
    function cssEscape(s) {
        if (window.CSS && typeof window.CSS.escape === 'function') {
            return window.CSS.escape(s);
        }
        return String(s).replace(/([!"#$%&'()*+,.\/:;<=>?@\[\\\]^`{|}~])/g, '\\$1');
    }
})();


/* =========================================================
   Version dropdown · 외부 클릭 시 닫기
   ========================================================= */
(function () {
    document.addEventListener('click', function (event) {
        const opened = document.querySelectorAll('.version-dropdown[open]');
        if (opened.length === 0) return;
        opened.forEach((dd) => {
            if (!dd.contains(event.target)) dd.removeAttribute('open');
        });
    });
})();

/* =========================================================
   Hero carousel · 가로 스와이프 + dot 네비게이션
   ========================================================= */
(function () {
    function init(carousel) {
        const track = carousel.querySelector('.hero-carousel__track');
        if (!track) return;
        const slides = Array.from(track.querySelectorAll('.hero-carousel__slide'));
        const dots = Array.from(carousel.querySelectorAll('.hero-carousel__dot'));
        if (slides.length === 0 || dots.length === 0) return;

        // Dot 클릭 → 해당 슬라이드로 스크롤
        dots.forEach((dot, index) => {
            dot.addEventListener('click', () => {
                const target = slides[index];
                if (!target) return;
                track.scrollTo({
                    left: target.offsetLeft - track.offsetLeft,
                    behavior: 'smooth'
                });
            });
        });

        // 스크롤 위치에 따라 active dot 갱신
        const observer = new IntersectionObserver((entries) => {
            entries.forEach((entry) => {
                if (!entry.isIntersecting) return;
                if (entry.intersectionRatio < 0.55) return;
                const index = slides.indexOf(entry.target);
                if (index < 0) return;
                dots.forEach((d) => d.classList.remove('is-active'));
                dots[index].classList.add('is-active');
            });
        }, { root: track, threshold: [0.55, 0.9] });

        slides.forEach((slide) => observer.observe(slide));

        // 좌우 화살표 키로도 이동 (트랙이 포커스되어 있을 때)
        track.addEventListener('keydown', (event) => {
            if (event.key !== 'ArrowLeft' && event.key !== 'ArrowRight') return;
            event.preventDefault();
            const currentIndex = dots.findIndex((d) => d.classList.contains('is-active'));
            const delta = event.key === 'ArrowRight' ? 1 : -1;
            const nextIndex = Math.min(Math.max(currentIndex + delta, 0), slides.length - 1);
            dots[nextIndex]?.click();
        });
    }

    document.querySelectorAll('.hero-carousel').forEach(init);
})();


/* =========================================================
   Home hero · 메시지 흐름 다이어그램 (Producer → Broker → Consumer)
   ---------------------------------------------------------
   prev/next 버튼 또는 노드 직접 클릭으로 단계 전환.
   forward 이동 시 직전 단계의 link 위에서 메시지 도트가 한 번 흐른다.
   ========================================================= */
(function () {
    function init(diagram) {
        const nodes = Array.from(diagram.querySelectorAll('.flow-diagram__node'));
        const links = Array.from(diagram.querySelectorAll('.flow-diagram__link'));
        const panels = Array.from(diagram.querySelectorAll('.flow-diagram__panel'));
        const prevBtn = diagram.querySelector('[data-action="prev"]');
        const nextBtn = diagram.querySelector('[data-action="next"]');
        const progressCurrent = diagram.querySelector('.flow-diagram__progress-current');
        const total = nodes.length;
        if (total === 0 || panels.length !== total) return;

        let step = 0;

        function apply() {
            nodes.forEach((node, idx) => {
                node.classList.toggle('is-active', idx === step);
            });
            panels.forEach((panel, idx) => {
                panel.classList.toggle('is-active', idx === step);
            });
            links.forEach((link, idx) => {
                link.classList.toggle('is-active', idx < step);
                link.classList.toggle('is-streaming', idx === step);
            });

            if (progressCurrent) {
                progressCurrent.textContent = String(step + 1).padStart(2, '0');
            }
        }

        function go(target) {
            const wrapped = ((target % total) + total) % total;
            if (wrapped === step) return;
            step = wrapped;
            apply();
        }

        nodes.forEach((node, idx) => {
            node.addEventListener('click', () => go(idx));
        });
        if (prevBtn) prevBtn.addEventListener('click', () => go(step - 1));
        if (nextBtn) nextBtn.addEventListener('click', () => go(step + 1));

        apply();
    }

    document.querySelectorAll('[data-flow-diagram]').forEach(init);
})();

/* =========================================================
   docs-sidenav — 가로 스크롤 모드에서 현재 페이지(active)가
   viewport 가운데에 오도록 scrollLeft 조정.
   페이지 이동 시 sidenav가 매번 0으로 리셋되어 사용자가 자기 위치를 잃는 문제 보정.
   데스크톱(세로 stack)에서는 overflow가 없어 scrollLeft 변경이 무시됨 → 항상 실행 안전.
   ========================================================= */
(function () {
    function focusActiveSidenavItem() {
        document.querySelectorAll('.docs-sidenav .sidenav').forEach(function (track) {
            const active = track.querySelector('.sidenav__head--active');
            if (!active) return;

            const target = active.closest('.sidenav__group') || active;
            const trackRect = track.getBoundingClientRect();
            const targetRect = target.getBoundingClientRect();
            const delta =
                (targetRect.left + targetRect.right) / 2 -
                (trackRect.left + trackRect.right) / 2;

            track.scrollLeft += delta;
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', focusActiveSidenavItem);
    } else {
        focusActiveSidenavItem();
    }
})();
