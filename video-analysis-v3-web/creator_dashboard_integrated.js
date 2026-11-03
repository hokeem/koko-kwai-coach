/* Additional operational detail for the standalone Creator dashboard. */
(() => {
  if (typeof dashboardData === 'undefined') return;
  const originalLoad = loadDashboard;
  const originalRender = renderDashboard;
  let extras = null;
  let generation = 0;
  let exportsByType = {};
  let workbookRows = {};
  let expandedMetric = '';
  let neverExpanded = false;
  const expandedSections = new Set();
  const layout = document.createElement('style');
  layout.textContent = '.dashboard-app{grid-template-columns:minmax(0,1fr)} .dashboard-sidebar{display:none} .dashboard-overall-group .dashboard-kpis{grid-template-columns:repeat(3,minmax(0,1fr))}.dashboard-overall-group .dashboard-kpi{min-width:0;min-height:160px;grid-template-rows:1fr auto;cursor:pointer}.dashboard-overall-group .dashboard-kpi:focus-within,.dashboard-overall-group .dashboard-kpi:hover{border-color:#ffab7c}.dashboard-overall-group .dashboard-kpi-note{line-height:1.4;overflow-wrap:anywhere}.dashboard-kpi-actions{grid-column:1/-1;display:flex;align-items:center;justify-content:space-between;gap:10px;min-width:0;flex-wrap:wrap}.dashboard-kpi-actions button{width:auto;min-height:34px;padding:0 12px;white-space:nowrap;font-size:12px;line-height:1.2}.dashboard-kpi-actions [data-metric-toggle]{border:0;background:transparent;color:#b54d10;font-weight:800}.dashboard-kpi-actions [data-dashboard-export]{border:1px solid #f4b995;background:#fff7f0;color:#ad470b}.dashboard-metric-list{min-width:0}.dashboard-metric-list[hidden]{display:none!important}@media(max-width:700px){.dashboard-overall-group .dashboard-kpis{grid-template-columns:1fr}.dashboard-overall-group .dashboard-kpi{min-height:140px}}';
  document.head.append(layout);
  const esc = value => dashboardEsc(String(value ?? ''));
  const date = value => value ? timeText(value) : '未记录';
  const title = value => esc(value || '未命名');
  const link = (url, label) => {
    if (!url) return '—';
    try {
      const parsed = new URL(String(url || ''), location.origin);
      return ['https:', 'http:'].includes(parsed.protocol)
        ? `<a href="${esc(parsed.href)}" target="_blank" rel="noopener noreferrer">${esc(label)}</a>` : '—';
    } catch (_) { return '—'; }
  };
  const table = (heads, rows, empty = '暂无记录') => rows.length
    ? `<div style="overflow:auto"><table class="dashboard-channel-table"><thead><tr>${heads.map(x => `<th>${esc(x)}</th>`).join('')}</tr></thead><tbody>${rows.map(r => `<tr>${r.map(c => `<td>${c}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`
    : `<div class="dashboard-empty">${esc(empty)}</div>`;
  const section = (id, name, note, content) => ['integrated-authors', 'integrated-behavior', 'integrated-production'].includes(id)
    ? `<details class="dashboard-panel" id="${id}" data-collapsible ${expandedSections.has(id) ? 'open' : ''}><summary><strong>${esc(name)}</strong> <span class="dashboard-panel-meta">${esc(note)}</span></summary><div style="margin-top:16px">${content}</div></details>`
    : `<article class="dashboard-panel" id="${id}"><div class="dashboard-panel-head"><h2>${esc(name)}</h2><span class="dashboard-panel-meta">${esc(note)}</span></div>${content}</article>`;
  const open = (label, content) => `<details><summary>${esc(label)}</summary>${content}</details>`;
  const inRange = value => dashboardInsideRange(value);
  const identity = item => String(item?.account_id || item?.creator_id || item?.phone || item?.kwai_id || '').toLowerCase();
  const aliases = item => new Set([item?.account_id, item?.creator_id, item?.phone, item?.kwai_id, item?.uid, item?.detected_kwai_id, item?.creator_profile_kwai_id, ...(item?.login_aliases || [])].filter(Boolean).map(x => String(x).trim().replace(/^@/, '').toLowerCase()));
  const matches = (a, b) => { const keys = aliases(a); return [...aliases(b)].some(key => keys.has(key)); };
  dashboardRenderScriptTable = rows => table(['排名','脚本标题','打开次数','去重打开作者','已回传'], rows.map((row,index) => [esc(index + 1), link(row.id ? `https://kokocomedy.com/script/${encodeURIComponent(row.id)}` : '', row.title || row.id), esc(row.opens), esc(row.authors.size), esc(row.returns)]), '此分类暂无脚本');

  // Fetch all pages; silently truncating gives incorrect totals and rankings.
  loadDashboardSubmissions = async function (first) {
    const rows = Array.isArray(first.submissions) ? first.submissions.slice() : [];
    let page = first;
    while (page.has_more && rows.length < Number(first.total || Infinity)) {
      const offset = rows.length;
      page = await dashboardApi(`/api/creator-admin/submissions?limit=300&offset=${offset}`);
      const batch = Array.isArray(page.submissions) ? page.submissions : [];
      if (!batch.length) throw new Error('回传分页不完整，请重试');
      rows.push(...batch);
    }
    return rows;
  };

  function renderExtras() {
    const root = document.querySelector('#dashboard-board .dashboard-content');
    if (!root || !dashboardData) return;
    root.querySelector('#integrated-detail')?.remove();
    const data = dashboardData;
    const actions = data.returns.filter(x => inRange(x.created_at || x.submitted_at));
    const authors = data.accounts.filter(a => creatorAccountIsRegistered(a) && !analyticsIsTestUser(a));
    const users = data.analytics?.users || [];
    const authorRows = authors.filter(a => inRange(a.registered_at || a.last_registered_at)).sort((a,b) => String(b.registered_at || b.last_registered_at).localeCompare(String(a.registered_at || a.last_registered_at))).map(a => {
      const key = identity(a), u = users.find(x => matches(x, a));
      const returns = actions.filter(x => matches(x, a));
      const profile = (extras?.creators?.creators || []).find(p => matches(p, a));
      const channel = (extras?.campaign?.rows || []).find(row => (row.new_creator_accounts || []).some(person => matches(person, a)));
      const convertedOpens = returns.filter(r => r.submission_type === 'independent').length * 2;
      return [title(a.display_name || a.account_id), esc(a.kwai_id || '未记录'), esc(date(a.registered_at || a.last_registered_at)), esc(channel?.source || a.source || '未归因'), link(profile?.kwai_url || a.kwai_url || a.profile_url || a.share_url, '主页'), esc(Number(u?.script_share_open_count || 0) + convertedOpens), esc(returns.length)];
    });
    const endDay = dashboardDateKey(new Date());
    const beginDate = new Date(`${endDay}T00:00:00+08:00`);
    beginDate.setUTCDate(beginDate.getUTCDate() - dashboardRange + 1);
    const period = dashboardRange === 0 ? `累计全部可用记录 · 截至 ${endDay} · 北京时间` : `${dashboardDateKey(beginDate)} 至 ${endDay} · 北京时间`;
    const retainedHistoryNote = dashboardRange === 0 ? `行为与渠道仅覆盖仍保留的事件${data.summary?.history_start ? `（最早 ${date(data.summary.history_start)}）` : ''}；更早已清理的日志无法恢复。` : '';
    const historicalIds = data.summary?.historical_script_opened_ids;
    const historicalSet = new Set(historicalIds || []);
    const historicalRows = Array.isArray(historicalIds) ? data.scripts.filter(s => !historicalSet.has(String(s.entry_id || s.id || ''))).map(s => [title(s.title || s.entry_id), link(s.entry_id ? `https://kokocomedy.com/script/${encodeURIComponent(s.entry_id)}` : '', '查看脚本'), esc(s.published === false ? '未发布' : '已发布')]) : [];
    const independentActions = actions.filter(r => r.submission_type === 'independent');
    const activeUsers = users.filter(u => !analyticsIsTestUser(u) && (u.has_activity || Number(u.platform_open_count || 0) || Number(u.script_share_open_count || 0) || (u.recent_events || []).length || independentActions.some(r => matches(r, u))));
    for (const author of authors) {
      if (independentActions.some(r => matches(r, author)) && !activeUsers.some(u => matches(u, author))) activeUsers.push(author);
    }
    const independentFor = u => actions.filter(r => r.submission_type === 'independent' && matches(r, u)).length;
    const activityRows = activeUsers.map(u => [title(u.display_name || u.account_id), esc(u.kwai_id || '—'), esc(u.platform_open_count || 0), esc(Number(u.script_share_open_count || 0) + independentFor(u) * 2), u.platform_duration_seconds == null ? '—' : esc(Math.round(u.platform_duration_seconds)), u.script_duration_seconds == null ? '—' : esc(Math.round(u.script_duration_seconds))]);
    workbookRows = {
      active: activeUsers.map(u => { const real = Number(u.script_share_open_count || 0), converted = independentFor(u) * 2; return [String(u.account_id || ''), String(u.kwai_id || ''), String(u.display_name || ''), Number(u.platform_open_count || 0), real, converted, real + converted, Number(u.platform_duration_seconds || 0), Number(u.script_duration_seconds || 0)]; }),
      returns: actions.map(r => { const account = authors.find(a => matches(a, r)); return [String(r.created_at || r.submitted_at || ''), String(r.account_id || r.creator_id || account?.account_id || ''), String(r.creator_profile_kwai_id || r.detected_kwai_id || account?.kwai_id || ''), String(r.creator_profile_name || r.creator_name || account?.display_name || ''), r.submission_type === 'independent' ? '自主回传' : '脚本回传', String(r.video_url || ''), r.entry_id ? `https://kokocomedy.com/script/${encodeURIComponent(r.entry_id)}` : '', String(r.status || 'pending_review')]; })
    };
    workbookRows.opens = workbookRows.active.filter(row => row[6] > 0).map(row => [row[0], row[1], row[2], row[4], row[5], row[6]]);
    const openRows = workbookRows.opens.map(row => [esc(row[2] || row[0]), esc(row[1] || '—'), esc(row[3]), esc(row[4]), esc(row[5])]);
    const reviewRows = actions.slice().sort((a,b) => String(b.created_at).localeCompare(String(a.created_at))).map(r => [esc(date(r.created_at || r.submitted_at)), title(r.creator_profile_name || r.creator_name || r.display_name || r.detected_kwai_id || r.creator_id), title(r.script_title || r.entry_id), link(r.entry_id ? `https://kokocomedy.com/script/${encodeURIComponent(r.entry_id)}` : '', '脚本'), link(r.video_url, '视频'), esc(({pending_review:'待审核',approved:'已通过',rejected:'未通过'})[r.status] || r.status || '待审核'), esc(r.review_reason || r.rejection_reason || r.reason || '—')]);
    const counts = actions.reduce((acc, r) => { const s = r.status || 'pending_review'; acc[s] = (acc[s] || 0) + 1; return acc; }, {});
    const production = extras?.production;
    const productionSummary = production?.summary || {};
    const productionRows = (production?.days || []).filter(d => inRange(`${d.date}T12:00:00+08:00`)).map(d => [esc(d.date), esc(d.script_count), esc(d.review_count), esc(d.edited_count), open('查看脚本', (d.items || []).map(i => `<div>${title(i.title)} · ${esc(i.completed_at_bj)} · ${link(i.video_url,'来源视频')} · ${i.reviewed?'已复盘':'未复盘'} · ${i.edited?'已编辑':'未编辑'}</div>`).join(''))]);
    const campaign = extras?.campaign;
    const campaignRows = (campaign?.rows || []).map(r => [esc(r.source + (r.source === 'inbox' ? '' : ' / ' + (r.campaign || ''))), esc(r.visits), esc(r.unique_visitors), esc(r.new_creators), esc(r.active_creators), esc(r.script_opens), esc(r.submissions), open(`已识别 ${(r.visit_accounts || []).length} 人 · 未识别 ${r.unidentified_visits || 0} 次`, (r.visit_accounts || []).map(p => `<div>${title(p.display_name)} · ${esc(p.kwai_id || p.account_id)} · ${esc(p.visits)} 次 · ${esc(date(p.last_visit))}</div>`).join('') || '无已识别账号')]);
    exportsByType = {authors: [['作者','Kwai ID','注册时间','渠道','主页','脚本打开（含折算）','匹配回传'], ...authorRows], reviews: [['提交时间','作者','脚本','脚本链接','视频链接','状态','原因'], ...reviewRows]};
    const panel = document.createElement('div');
    panel.id = 'integrated-detail';
    panel.style.display = 'grid'; panel.style.gap = '16px';
    const errors = extras?.errors || [];
    panel.innerHTML = `${retainedHistoryNote ? `<div class="dashboard-empty">${esc(retainedHistoryNote)}</div>` : ''}${errors.length ? `<div class="dashboard-empty">${esc(errors.join('；'))}</div>` : ''}` +
      section('integrated-channels', '推广渠道明细', dashboardRange === 0 ? '累计全部 · 巴西时间，首次渠道归因' : `最近 ${dashboardRange} 天 · 巴西时间，首次渠道归因`, campaign ? table(['渠道 / 活动','访问次数','去重访客','新增作者','活跃作者','脚本打开','回传','访问账号'], campaignRows) : '<div class="dashboard-empty">渠道数据加载中或暂不可用</div>') +
      section('integrated-authors', '新增作者明细', `${period}，打开和回传为所选周期`, '<button type="button" data-dashboard-export="authors">导出作者明细</button>' + table(['作者','Kwai ID','注册时间','渠道','主页','脚本打开（含折算）','匹配回传'], authorRows)) +
      section('integrated-behavior', '作者行为明细', `${period} · 停留时间：秒`, '<button type="button" data-dashboard-export="active">导出活跃作者 Excel</button>' + table(['作者','Kwai ID','平台打开','脚本打开（含折算）','平台停留','脚本停留'], activityRows)) +
      section('integrated-production', '脚本生产', '生成、复盘和整稿编辑 · 北京时间', production ? `<p class="dashboard-panel-meta">近 24 小时 ${esc(productionSummary.last_24h)} · 近 7 天 ${esc(productionSummary.last_7d)} · 近 30 天 ${esc(productionSummary.last_30d)} · 历史累计 ${esc(productionSummary.all_time)} · 近 30 天复盘 ${esc(productionSummary.review_count_30d)} · 近 30 天编辑 ${esc(productionSummary.edited_count_30d)}</p>${table(['日期','生成','复盘','编辑','脚本明细'],productionRows)}` : '<div class="dashboard-empty">生产数据加载中或暂不可用</div>');
    root.append(panel);
    panel.querySelectorAll('[data-collapsible]').forEach(detail => detail.addEventListener('toggle', () => {
      if (!detail.isConnected) return;
      if (detail.open) expandedSections.add(detail.id); else expandedSections.delete(detail.id);
    }));
    const behavior = panel.querySelector('#integrated-behavior');
    if (behavior) panel.prepend(behavior);
    const scriptPanel = [...root.children].find(x => x.querySelector?.(':scope > .dashboard-panel-head > h2')?.textContent === '脚本表现');
    if (scriptPanel) {
      const wrapper = document.createElement('details');
      wrapper.className = 'dashboard-panel';
      wrapper.id = 'integrated-scripts';
      wrapper.open = expandedSections.has(wrapper.id);
      wrapper.innerHTML = '<summary><strong>脚本表现</strong></summary>';
      scriptPanel.querySelector('.dashboard-panel-head')?.remove();
      while (scriptPanel.firstChild) wrapper.append(scriptPanel.firstChild);
      wrapper.addEventListener('toggle', () => {
        if (!wrapper.isConnected) return;
        if (wrapper.open) expandedSections.add(wrapper.id); else expandedSections.delete(wrapper.id);
      });
      scriptPanel.remove();
      root.append(wrapper);
    }
    const never = document.createElement('details');
    never.id = 'integrated-never';
    never.className = 'dashboard-panel';
    never.open = neverExpanded;
    never.innerHTML = `<summary><strong>没有历史打开记录的脚本</strong> <span class="dashboard-panel-meta">${historicalRows.length} 条</span></summary><p class="dashboard-panel-meta">仅覆盖保留的历史事件${data.summary?.history_start ? `，最早 ${esc(date(data.summary.history_start))}` : ''}；不代表更早从未打开。</p>${Array.isArray(historicalIds) ? table(['脚本','链接','状态'],historicalRows) : '<div class="dashboard-empty">历史事件接口尚未提供数据</div>'}`;
    never.addEventListener('toggle', () => { neverExpanded = never.open; });
    root.append(never);
    // Restrict each stage to the same newly registered authors; event order is not assumed.
    const conversion = [...root.querySelectorAll('.dashboard-panel')].find(x => x.querySelector('h2')?.textContent === '创作者转化');
    if (conversion) {
      const cohort = authors.filter(a => inRange(a.registered_at || a.last_registered_at));
      const opened = cohort.filter(a => users.some(u => matches(u,a) && Number(u.script_share_open_count || 0) > 0) || actions.some(r => r.submission_type === 'independent' && matches(r,a)));
      const returned = opened.filter(a => actions.some(r => matches(r,a)));
      const stages = [['新增作者',cohort.length],['打开脚本',opened.length],['有回传',returned.length]];
      conversion.innerHTML = `<div class="dashboard-panel-head"><h2>新作者行为</h2></div><div class="dashboard-funnel">${stages.map(([label,value],i) => `<div class="dashboard-funnel-step"><span class="dashboard-funnel-label">${esc(label)}</span><div class="dashboard-funnel-bar">${value}</div><div class="dashboard-funnel-rate">${i && stages[i-1][1] ? `${Math.round(value / stages[i-1][1] * 1000) / 10}%` : '—'}</div></div>`).join('')}</div><p class="dashboard-panel-meta">同一批周期内新注册作者，统计周期内的行为；不判断先后顺序。账号无法匹配的回传不计入。访客到注册的转化暂无法可靠关联。</p>`;
    }
    const channel = [...root.querySelectorAll('.dashboard-panel')].find(x => x.querySelector('h2')?.textContent === '渠道表现');
    if (channel) channel.innerHTML = `<div class="dashboard-panel-head"><h2>渠道表现</h2><span class="dashboard-panel-meta">${dashboardRange === 0 ? '累计全部' : `近 ${dashboardRange} 天`} · 巴西时间</span></div>${campaign ? table(['渠道 / 活动','访问次数','去重访客','新增作者','活跃作者','脚本打开','回传'], campaignRows.map(row => row.slice(0,7)), '该周期暂无渠道记录') : '<div class="dashboard-empty">渠道数据加载中或暂不可用</div>'}`;
    const kpis = root.querySelectorAll('.dashboard-kpi');
    if (kpis[1]) {
      const value = kpis[1].querySelector('.dashboard-kpi-value');
      const note = kpis[1].querySelector('.dashboard-kpi-note');
      if (value) value.textContent = String(activeUsers.length);
      if (note) note.textContent = '周期内有浏览、点击或自主回传';
    }
    if (kpis[0] && dashboardRange === 0) {
      const note = kpis[0].querySelector('.dashboard-kpi-note');
      if (note) note.textContent = '累计全部注册账号 · 按注册日期去重';
    }
    if (kpis[2]) {
      const opens = users.reduce((sum, u) => sum + (analyticsIsTestUser(u) ? 0 : Number(u.script_share_open_count || 0)), 0);
      const independent = actions.filter(s => s.submission_type === 'independent').length;
      const value = kpis[2].querySelector('.dashboard-kpi-value');
      const note = kpis[2].querySelector('.dashboard-kpi-note');
      if (value) value.textContent = String(opens + independent * 2);
      if (note) note.textContent = `真实打开 ${opens} 次 + 自主回传 ${independent} 条 × 2；未计入脚本排行`;
    }
    if (kpis[3]) {
      const label = kpis[3].querySelector('.dashboard-kpi-label');
      const value = kpis[3].querySelector('.dashboard-kpi-value');
      const note = kpis[3].querySelector('.dashboard-kpi-note');
      if (label) label.textContent = '视频回传';
      if (value) value.textContent = String(actions.length);
      if (note) note.textContent = '所选周期 · 回传记录数';
    }
    for (const [index, kind] of [[1, 'active'], [2, 'opens'], [3, 'returns']]) {
      const card = kpis[index];
      if (!card) continue;
      card.dataset.dashboardMetric = kind;
      const actionsBar = document.createElement('div');
      actionsBar.className = 'dashboard-kpi-actions';
      actionsBar.innerHTML = `<button type="button" data-metric-toggle="${kind}" aria-controls="dashboard-metric-${kind}" aria-expanded="${expandedMetric === kind}">${expandedMetric === kind ? '收起明细 ↑' : '查看明细 ↓'}</button><button type="button" data-dashboard-export="${kind}">导出 Excel</button>`;
      card.append(actionsBar);
    }
    const kpiGrid = root.querySelector('.dashboard-kpis');
    const newAuthorCard = kpis[0];
    const cohortLayout = root.querySelector('.dashboard-layout-two');
    const trendLayout = root.querySelector('.dashboard-layout-trend');
    const newAuthors = panel.querySelector('#integrated-authors');
    if (kpiGrid && newAuthorCard) {
      const overall = document.createElement('section');
      overall.className = 'dashboard-overall-group';
      overall.innerHTML = `<h2 style="margin:0;font-size:30px">作者总体行为数据</h2><p class="dashboard-panel-meta" style="margin:0">${esc(period)}</p>`;
      kpiGrid.before(overall);
      overall.append(kpiGrid);
      const metricLists = document.createElement('div');
      metricLists.className = 'dashboard-metric-lists';
      metricLists.innerHTML = `<section class="dashboard-panel dashboard-metric-list" id="dashboard-metric-active" ${expandedMetric === 'active' ? '' : 'hidden'}><div class="dashboard-panel-head"><h2>活跃作者明细</h2><span class="dashboard-panel-meta">${esc(period)}</span></div>${table(['作者','Kwai ID','平台打开','脚本打开（含折算）','平台停留(秒)','脚本停留(秒)'], activityRows)}</section><section class="dashboard-panel dashboard-metric-list" id="dashboard-metric-opens" ${expandedMetric === 'opens' ? '' : 'hidden'}><div class="dashboard-panel-head"><h2>脚本打开明细</h2><span class="dashboard-panel-meta">真实打开 + 自主回传折算</span></div>${table(['作者','Kwai ID','真实打开','自主回传折算','合计'], openRows)}</section><section class="dashboard-panel dashboard-metric-list" id="dashboard-metric-returns" ${expandedMetric === 'returns' ? '' : 'hidden'}><div class="dashboard-panel-head"><h2>视频回传明细</h2><span class="dashboard-panel-meta">${esc(period)}</span></div>${table(['提交时间','作者','脚本','脚本链接','视频链接','审核状态','原因'], reviewRows)}</section>`;
      overall.append(metricLists);
      if (behavior) overall.append(behavior);
      overall.style.display = 'grid';
      overall.style.gap = '16px';
      const newcomers = document.createElement('section');
      newcomers.className = 'dashboard-newcomer-group';
      newcomers.style.display = 'grid';
      newcomers.style.gap = '16px';
      newcomers.innerHTML = '<h2 style="margin:0;font-size:22px">新作者行为汇总</h2>';
      overall.after(newcomers);
      const newGrid = document.createElement('div');
      newGrid.className = 'dashboard-kpis';
      newGrid.style.gridTemplateColumns = 'minmax(0,1fr)';
      newGrid.append(newAuthorCard);
      newcomers.append(newGrid);
      if (cohortLayout) newcomers.append(cohortLayout);
      if (trendLayout) newcomers.append(trendLayout);
      if (newAuthors) newcomers.append(newAuthors);
      if (channel) channel.insertAdjacentHTML('beforeend', '<p class="dashboard-panel-meta">新增作者按渠道归因；活跃、脚本打开和回传包含新老作者。</p>');
    }
  }
  renderDashboard = function () {
    if (!dashboardData) return originalRender();
    const allReturns = dashboardData.returns;
    try {
      dashboardData.returns = allReturns.filter(x => inRange(x.created_at || x.submitted_at));
      originalRender();
    } finally { dashboardData.returns = allReturns; }
    renderExtras();
  };
  document.addEventListener('click', event => {
    if (!event.target.closest('[data-dashboard-export]')) {
      const card = event.target.closest('[data-dashboard-metric]');
      if (card) {
        const kind = card.dataset.dashboardMetric;
        expandedMetric = expandedMetric === kind ? '' : kind;
        document.querySelectorAll('.dashboard-metric-list').forEach(list => { list.hidden = list.id !== `dashboard-metric-${expandedMetric}`; });
        document.querySelectorAll('[data-metric-toggle]').forEach(toggle => {
          const open = toggle.dataset.metricToggle === expandedMetric;
          toggle.setAttribute('aria-expanded', String(open));
          toggle.textContent = open ? '收起明细 ↑' : '查看明细 ↓';
        });
        if (expandedMetric) document.querySelector(`#dashboard-metric-${expandedMetric}`)?.scrollIntoView({block:'nearest',behavior:'smooth'});
        return;
      }
    }
    const button = event.target.closest('[data-dashboard-export]');
    if (!button) return;
    const kind = button.dataset.dashboardExport;
    if (kind === 'active' || kind === 'opens' || kind === 'returns') {
      button.disabled = true;
      fetch('/api/creator-admin/dashboard-export', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({kind, rows: workbookRows[kind] || []})})
        .then(async response => { if (!response.ok) throw new Error((await response.json()).error || '导出失败'); return response.blob(); })
        .then(blob => { const url = URL.createObjectURL(blob); const a = document.createElement('a'); a.href = url; a.download = `koko-${kind}-${dashboardRange || 'all'}.xlsx`; a.click(); setTimeout(() => URL.revokeObjectURL(url), 1000); })
        .catch(error => alert(error.message || '导出失败'))
        .finally(() => { button.disabled = false; });
      return;
    }
    const rows = exportsByType[kind] || [];
    const csv = rows.map(row => row.map(value => {
      const element = document.createElement('div'); element.innerHTML = String(value ?? '');
      let text = element.querySelector('a')?.href || element.textContent || '';
      if (/^[=+\-@\t\r]/.test(text)) text = `'${text}`;
      return `"${text.replace(/"/g, '""')}"`;
    }).join(',')).join('\r\n');
    const url = URL.createObjectURL(new Blob(['\ufeff', csv], {type: 'text/csv;charset=utf-8'}));
    const a = document.createElement('a'); a.href = url; a.download = `koko-${button.dataset.dashboardExport}.csv`; a.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  });
  loadDashboard = async function (reuseExisting = false) {
    const request = ++generation;
    extras = null;
    while (dashboardLoading && request === generation) await new Promise(resolve => setTimeout(resolve, 100));
    if (request !== generation) return;
    if (!reuseExisting || !dashboardData) await originalLoad();
    if (request !== generation || !dashboardData || dashboardDemoEnabled) return;
    const snapshot = dashboardData;
    try {
      const errors = [];
      const optional = async (url, label) => { try { const result = await dashboardApi(url); if (result.from_cache) errors.push(`${label}显示缓存`); return result; } catch (_) { errors.push(`${label}暂不可用`); return null; } };
      extras = {campaign: null, production: null, creators: null, errors};
      const status = document.querySelector('#status');
      if (status?.textContent?.includes('缓存')) errors.push('部分核心数据来自缓存');
      if (status) status.textContent = '核心数据已加载，正在补充渠道及明细…';
      extras.campaign = await optional(`/api/creator-admin/campaign-dashboard?days=${dashboardRange || 90}&all=${dashboardRange === 0 ? 1 : 0}`, '渠道数据');
      if (request !== generation) return;
      renderDashboard();
      while (snapshot.scripts.length < snapshot.scriptTotal) {
        const page = await optional(`/api/creator-admin/scripts?limit=1000&scope=all&offset=${snapshot.scripts.length}`, '脚本列表');
        if (request !== generation) return;
        if (!page || !Array.isArray(page.entries) || !page.entries.length || Number(page.offset || 0) !== snapshot.scripts.length) { errors.push('脚本列表未加载完整，排行榜仅覆盖已加载脚本'); break; }
        snapshot.scripts.push(...page.entries);
      }
      extras.production = await optional('/api/creator-admin/production-stats', '脚本生产数据');
      if (request !== generation) return;
      renderDashboard();
      extras.creators = await optional('/api/creator-admin/creators', '作者主页');
      if (request !== generation) return;
      renderDashboard();
      if (status) status.textContent = errors.length ? '部分数据暂不可用，请查看各版块提示。' : '数据已更新。';
    } finally { /* A later range selection supersedes this request. */ }
  };
  if (dashboardDemoEnabled && dashboardData) renderDashboard();
  if (dashboardLoading && !dashboardDemoEnabled) loadDashboard(true);
})();
