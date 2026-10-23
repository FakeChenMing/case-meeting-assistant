const fs=require('fs'),vm=require('vm'),assert=require('assert');
const html=fs.readFileSync(__dirname+'/../skills/case-meeting-assistant/assets/runtime/index.html','utf8');
const code=html.slice(html.indexOf('let lifecycleSession='),html.indexOf('function render(){'));
const nodes={},panels=[{},{}],controls=[{},{}],drafts=new Map();
const node=()=>({value:'',textContent:'',hidden:false,disabled:false,open:false,children:[],replaceChildren(){this.children=[]},append(...x){this.children.push(...x)},close(){this.open=false},pause(){this.paused=true},removeAttribute(){},load(){}});
const ctx={state:{view:'active',session:'old',phase:'ended',model:'gpt-6-astra',effort:'medium',jobs:[],capture:{}},
 $:id=>nodes[id]??=node(),el:(tag,text)=>({...node(),textContent:text}),document:{querySelectorAll:s=>s==='[data-session-panel]'?panels:controls},
 sessionStorage:{setItem:(k,v)=>drafts.set(k,v),getItem:k=>drafts.get(k)},selectionId:'',editing:{},playlist:[],rangeEnd:null,effortLabel:x=>x};
vm.createContext(ctx);vm.runInContext(code+';renderSessionView()',ctx);
nodes.ask.value='旧案尚未提交问题';nodes.case.value='旧案路径';nodes.pack.value='旧交接';nodes.correctionDialog.open=true;
ctx.state={view:'home',session:null,phase:'home',model:'gpt-6-astra',effort:'medium',recent_sessions:[{session:'old',matter:'旧虚构案',handoff_exists:true}]};
vm.runInContext('renderSessionView()',ctx);
assert(panels.every(p=>p.hidden)&&controls.every(p=>p.hidden));assert(!nodes.homePanel.hidden&&nodes.caseSelector.open);
assert.equal(nodes.ask.value,'');assert.equal(nodes.case.value,'');assert.equal(nodes.pack.value,'');assert(nodes.mainPlayer.paused);assert(!nodes.correctionDialog.open);
assert.equal(drafts.get('meeting-ask:old'),'旧案尚未提交问题');assert.equal(nodes.recentSessions.children.length,1);
ctx.state={view:'history',session:'old',phase:'ended',matter:'旧虚构案',model:'gpt-6-astra',effort:'medium',jobs:[],capture:{}};
vm.runInContext('renderSessionView()',ctx);
assert(panels.every(p=>!p.hidden)&&controls.every(p=>p.hidden));assert(!nodes.resumeHistory.hidden);assert(nodes.sessionNotice.textContent.includes('历史记录'));
ctx.state={...ctx.state,view:'active',phase:'ending',return_home_after_finish:true};vm.runInContext('renderSessionView()',ctx);
assert(nodes.goHome.disabled);assert(nodes.sessionNotice.textContent.includes('成功后自动返回首页'));
ctx.state={view:'active',session:'new',phase:'ready',model:'gpt-6-astra',effort:'medium',jobs:[],capture:{}};vm.runInContext('renderSessionView()',ctx);
assert.equal(nodes.ask.value,'');assert(controls.every(p=>!p.hidden));
console.log('PASS: home clears old case controls/fields/audio, history read-only, unsent draft scoped, pending closeout retained, new case blank');
