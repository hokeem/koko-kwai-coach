/* Additional operational detail for the standalone Creator dashboard. */
(() => {
  if (typeof dashboardData === 'undefined') return;
  const originalLoad = loadDashboard;
  const originalRender = renderDashboard;
  let extras = null;
  let generation = 0;
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
  const section = (id, name, note, content) => `<article class="dashboard-panel" id="${id}"><div class="dashboard-panel-head"><h2>${esc(name)}</h2><span class="dashboard-panel-meta">${esc(note)}</span></div>${content}</article>`;
  const open = (label, content) => `<details><summary>${esc(label)}</summary>${content}</details>`;
  const inRange = value => dashboardInsideRange(value);
  const identity = item => String(item?.account_id || item?.creator_id || item?.phone || item?.kwai_id || '').toLowerCase();
  const aliases = item => new Set([item?.account_id, item?.creator_id, item?.phone, item?.kwai_id, item?.uid, item?.detected_kwai_id, item?.creator_profile_kwai_id, ...(item?.login_aliases || [])].filter(Boolean).map(x => String(x).trim().replace(/^@/, '').toLowerCase()));
  const matches = (a, b) => { const keys = aliases(a); return [...aliases(b)].some(key => keys.has(key)); };
  dashboardRenderScriptTable = rows => table(['排名','脚本标题','打开次数','去重打开作者','已回传','审核通过'], rows.map((row,index) => [esc(index + 1), link(row.id ? `https://kokocomedy.com/script/${encodeURIComponent(row.id)}` : '', row.title || row.id), esc(row.opens), esc(row.authors.size), esc(row.returns), esc(row.approved || 0)]), '此分类暂无脚本');

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
      return [title(a.display_name || a.account_id), esc(a.kwai_id || '未记录'), esc(date(a.registered_at || a.last_registered_at)), esc(channel?.source || a.source || '未归因'), link(profile?.kwai_url || a.kwai_url || a.profile_url || a.share_url, '主页'), esc(u?.script_share_open_count || 0), esc(returns.length)];
    });
    const days = data.summary?.daily_usage || [];
    const usageRows = days.map(d => [esc(d.day), esc(d.active_users), esc(d.script_opens), esc(d.unique_scripts_opened), open('作者', (d.people || []).map(p => `<div>${esc(p.display_name || p.kwai_id || p.account_id)}</div>`).join('') || '无作者明细')]);
    const groupUsage = keyFor => {
      const groups = new Map();
      for (const day of days) {
        const key = keyFor(day.day), group = groups.get(key) || {people: new Set(), opens: 0};
        for (const person of day.people || []) { const id = identity(person); if (id) group.people.add(id); }
        group.opens += Number(day.script_opens || 0); groups.set(key, group);
      }
      return [...groups].sort((a,b) => b[0].localeCompare(a[0])).map(([key,g]) => [esc(key),esc(g.people.size),esc(g.opens)]);
    };
    const weekOf = day => { const d = new Date(`${day}T12:00:00Z`); d.setUTCDate(d.getUTCDate() - ((d.getUTCDay() + 6) % 7)); return d.toISOString().slice(0,10); };
    const historicalIds = data.summary?.historical_script_opened_ids;
    const historicalSet = new Set(historicalIds || []);
    const historicalRows = Array.isArray(historicalIds) ? data.scripts.filter(s => !historicalSet.has(String(s.entry_id || s.id || ''))).map(s => [title(s.title || s.entry_id), link(s.entry_id ? `https://kokocomedy.com/script/${encodeURIComponent(s.entry_id)}` : '', '查看脚本'), esc(s.published === false ? '未发布' : '已发布')]) : [];
    const activityRows = users.filter(u => !analyticsIsTestUser(u)).map(u => [title(u.display_name || u.account_id), esc(u.kwai_id || '—'), esc(u.platform_open_count || 0), esc(u.script_share_open_count || 0), u.platform_duration_seconds == null ? '—' : esc(Math.round(u.platform_duration_seconds)), u.script_duration_seconds == null ? '—' : esc(Math.round(u.script_duration_seconds)), u.click_counts ? open('功能点击', Object.entries(u.click_counts).map(([k,v]) => `<div>${esc(k)}: ${esc(v)}</div>`).join('') || '无记录') : '—']);
    const feedRows = [];
    for (const [profileId, state] of Object.entries(extras?.state?.state?.creators || {})) {
      const profile = (extras?.creators?.creators || []).find(p => p.profile_id === profileId) || {};
      for (const feed of state.feeds || []) {
        if (!inRange(feed.date || feed.feed_time)) continue;
        feedRows.push([title(profile.name || profile.kwai_id || profileId), esc(feed.date || feed.feed_time), link(feed.share_url || (feed.entry_id ? `https://kokocomedy.com/script/${encodeURIComponent(feed.entry_id)}` : ''), '脚本'), esc(feed.status || '未完成'), link(feed.return_url, '回传视频')]);
      }
    }
    const reviewRows = actions.slice().sort((a,b) => String(b.created_at).localeCompare(String(a.created_at))).map(r => [esc(date(r.created_at || r.submitted_at)), title(r.creator_profile_name || r.creator_name || r.display_name || r.detected_kwai_id || r.creator_id), title(r.script_title || r.entry_id), link(r.entry_id ? `https://kokocomedy.com/script/${encodeURIComponent(r.entry_id)}` : '', '脚本'), link(r.video_url, '视频'), esc(({pending_review:'待审核',approved:'已通过',rejected:'未通过'})[r.status] || r.status || '待审核'), esc(r.review_reason || r.rejection_reason || r.reason || '—')]);
    const counts = actions.reduce((acc, r) => { const s = r.status || 'pending_review'; acc[s] = (acc[s] || 0) + 1; return acc; }, {});
    const production = extras?.production;
    const productionSummary = production?.summary || {};
    const productionRows = (production?.days || []).filter(d => inRange(`${d.date}T12:00:00+08:00`)).map(d => [esc(d.date), esc(d.script_count), esc(d.review_count), esc(d.edited_count), open('查看脚本', (d.items || []).map(i => `<div>${title(i.title)} · ${esc(i.completed_at_bj)} · ${link(i.video_url,'来源视频')} · ${i.reviewed?'已复盘':'未复盘'} · ${i.edited?'已编辑':'未编辑'}</div>`).join(''))]);
    const campaign = extras?.campaign;
    const campaignRows = (campaign?.rows || []).map(r => [esc(r.source + (r.source === 'inbox' ? '' : ' / ' + (r.campaign || ''))), esc(r.visits), esc(r.unique_visitors), esc(r.new_creators), esc(r.active_creators), esc(r.script_opens), esc(r.submissions), open(`已识别 ${(r.visit_accounts || []).length} 人 · 未识别 ${r.unidentified_visits || 0} 次`, (r.visit_accounts || []).map(p => `<div>${title(p.display_name)} · ${esc(p.kwai_id || p.account_id)} · ${esc(p.visits)} 次 · ${esc(date(p.last_visit))}</div>`).join('') || '无已识别账号')]);
    const panel = document.createElement('div');
    panel.id = 'integrated-detail';
    panel.style.display = 'grid'; panel.style.gap = '16px';
    const errors = extras?.errors || [];
    panel.innerHTML = `${errors.length ? `<div class="dashboard-empty">${esc(errors.join('；'))}</div>` : ''}` +
      section('integrated-channels', '推广渠道明细', `最近 ${dashboardRange} 天 · 巴西时间，首次渠道归因`, campaign ? table(['渠道 / 活动','访问次数','去重访客','新增作者','活跃作者','脚本打开','回传','访问账号'], campaignRows) : '<div class="dashboard-empty">渠道数据加载中或暂不可用</div>') +
      section('integrated-authors', '新增作者明细', `近 ${dashboardRange} 天 · 北京时间，打开和回传为所选周期`, table(['作者','Kwai ID','注册时间','渠道','主页','打开脚本','匹配回传'], authorRows)) +
      section('integrated-usage', '作者使用统计', '巴西时间 · 根据所选周期内事件去重；首尾月份或周可能不完整', open('按月',table(['月份','活跃作者','脚本打开次数'],groupUsage(day => day.slice(0,7)))) + open('按周（周一开始）',table(['周开始','活跃作者','脚本打开次数'],groupUsage(weekOf))) + open('按日及作者明细',table(['日期','活跃作者','脚本打开次数','打开脚本数','作者明细'], usageRows))) +
      section('integrated-never', '没有历史打开记录的脚本', `仅覆盖保留的历史事件${data.summary?.history_start ? `，最早 ${date(data.summary.history_start)}` : ''}；不代表更早从未打开`, Array.isArray(historicalIds) ? table(['脚本','链接','状态'],historicalRows) : '<div class="dashboard-empty">历史事件接口尚未提供数据</div>') +
      section('integrated-behavior', '作者行为明细', '所选周期；停留时间以秒计，— 表示接口未提供', table(['作者','Kwai ID','平台打开','脚本打开','平台停留','脚本停留','功能点击'], activityRows)) +
      section('integrated-feed', '投喂记录', '所选周期 · 内容中台记录的投喂；与视频回传分开统计', extras?.state ? table(['作者','投喂日期','脚本','状态','回传链接'],feedRows) : '<div class="dashboard-empty">投喂数据加载中或暂不可用</div>') +
      section('integrated-production', '脚本生产', '生成、复盘和整稿编辑 · 北京时间', production ? `<p class="dashboard-panel-meta">近 24 小时 ${esc(productionSummary.last_24h)} · 近 7 天 ${esc(productionSummary.last_7d)} · 近 30 天 ${esc(productionSummary.last_30d)} · 历史累计 ${esc(productionSummary.all_time)} · 近 30 天复盘 ${esc(productionSummary.review_count_30d)} · 近 30 天编辑 ${esc(productionSummary.edited_count_30d)}</p>${table(['日期','生成','复盘','编辑','脚本明细'],productionRows)}` : '<div class="dashboard-empty">生产数据加载中或暂不可用</div>') +
      section('integrated-review', '回传审核明细', `所选周期 · 待审核 ${counts.pending_review || 0} · 已通过 ${counts.approved || 0} · 未通过 ${counts.rejected || 0}`, `<p><a href="/koko_admin_2026-kwai/reviews">进入回传审核</a></p>${table(['提交时间','作者','脚本','脚本链接','视频链接','状态','原因'],reviewRows)}`);
    root.append(panel);
    // Restrict each stage to the same newly registered authors; event order is not assumed.
    const conversion = [...root.querySelectorAll('.dashboard-panel')].find(x => x.querySelector('h2')?.textContent === '创作者转化');
    if (conversion) {
      const cohort = authors.filter(a => inRange(a.registered_at || a.last_registered_at));
      const opened = cohort.filter(a => users.some(u => matches(u,a) && Number(u.script_share_open_count || 0) > 0));
      const returned = opened.filter(a => actions.some(r => matches(r,a)));
      const approved = returned.filter(a => actions.some(r => matches(r,a) && r.status === 'approved'));
      const stages = [['新增作者',cohort.length],['打开脚本',opened.length],['有回传',returned.length],['审核通过',approved.length]];
      conversion.innerHTML = `<div class="dashboard-panel-head"><h2>新作者行为</h2></div><div class="dashboard-funnel">${stages.map(([label,value],i) => `<div class="dashboard-funnel-step"><span class="dashboard-funnel-label">${esc(label)}</span><div class="dashboard-funnel-bar">${value}</div><div class="dashboard-funnel-rate">${i && stages[i-1][1] ? `${Math.round(value / stages[i-1][1] * 1000) / 10}%` : '—'}</div></div>`).join('')}</div><p class="dashboard-panel-meta">同一批周期内新注册作者，统计周期内的行为；不判断先后顺序。账号无法匹配的回传不计入。访客到注册的转化暂无法可靠关联。</p>`;
    }
    const channel = [...root.querySelectorAll('.dashboard-panel')].find(x => x.querySelector('h2')?.textContent === '渠道表现');
    if (channel) channel.innerHTML = '<div class="dashboard-panel-head"><h2>渠道表现</h2></div><div class="dashboard-empty">真实渠道访问和归因数据见下方“推广渠道明细”。</div>';
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
      extras = {campaign: null, production: null, creators: null, state: null, errors};
      const status = document.querySelector('#status');
      if (status?.textContent?.includes('缓存')) errors.push('部分核心数据来自缓存');
      if (status) status.textContent = '核心数据已加载，正在补充渠道及明细…';
      extras.campaign = await optional(`/api/creator-admin/campaign-dashboard?days=${dashboardRange}`, '渠道数据');
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
      extras.state = await optional('/api/creator-admin/state', '投喂记录');
      if (request !== generation) return;
      renderDashboard();
      if (status) status.textContent = errors.length ? '部分数据暂不可用，请查看各版块提示。' : '数据已更新。';
    } finally { /* A later range selection supersedes this request. */ }
  };
  if (dashboardLoading && !dashboardDemoEnabled) loadDashboard(true);
})();
