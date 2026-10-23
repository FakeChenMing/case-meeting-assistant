const fs=require('fs'), vm=require('vm'), assert=require('assert');
const html=fs.readFileSync(__dirname+'/../skills/case-meeting-assistant/assets/runtime/index.html','utf8');
const code=html.slice(html.indexOf('let modelDraft=null'),html.indexOf('function render(){'));
function node(){return {textContent:'',value:'',disabled:false,checked:true,options:[],replaceChildren(){this.options=[];this.value=''},append(o){this.options.push(o);if(!this.value)this.value=o.value}}}
const nodes={},calls=[];
const ctx={state:{session:'S',model:'gpt-6.1-sol',effort:'high',model_selection:{model:'gpt-6.1-sol',effort:'high',revision:0},
  model_defaults:{model:'gpt-6.1-sol',effort:'high'},jobs:[],model_catalog:{fresh:true,fetched_at:1,models:[
    {model:'gpt-6.1-sol',label:'GPT-6.1-Sol',efforts:['low','high'],default_effort:'low'},
    {model:'alternative',label:'Alternative',efforts:['medium','high'],default_effort:'medium'}]}},
  $:id=>nodes[id]??=(node()),el:(tag,text)=>({...node(),textContent:text}),crypto:{randomUUID:()=> 'test-operation-123'},
  toast(){},async post(d){calls.push(d);return{notice:'saved'}},async refresh(){}};
vm.createContext(ctx);vm.runInContext(code,ctx);
vm.runInContext('renderModelSettings()',ctx);
assert.equal(nodes.modelChoice.value,'gpt-6.1-sol');assert.equal(nodes.effortChoice.value,'high');
nodes.modelChoice.value='alternative';vm.runInContext('modelDraftChanged(true)',ctx);
vm.runInContext('renderModelSettings();renderModelSettings()',ctx);
assert.equal(nodes.modelChoice.value,'alternative');assert(nodes.modelNotice.textContent.includes('尚未应用'));
assert.equal(nodes.effortChoice.options.length,2);assert(!nodes.effortChoice.options.some(o=>o.value==='low'));
assert.equal(calls.length,0,'changing a dropdown must never submit a request');
ctx.state.model_selection={model:'gpt-6.1-sol',effort:'low',revision:1};vm.runInContext('renderModelSettings()',ctx);
assert(nodes.modelApply.disabled);assert(nodes.modelNotice.textContent.includes('另一个页面'));
vm.runInContext('resetModelDraft()',ctx);assert.equal(nodes.effortChoice.value,'low');
ctx.state.jobs=[{state:'inflight',model:'gpt-6.1-sol',effort:'high'}];vm.runInContext('renderModelSettings()',ctx);
assert(nodes.modelNotice.textContent.includes('1个在途或排队'));
ctx.state.model_catalog.fresh=false;vm.runInContext('renderModelSettings()',ctx);assert(nodes.modelApply.disabled);
ctx.state.model_catalog.fresh=true;ctx.state.session='NEW';ctx.state.model_selection={model:'alternative',effort:'high',revision:0};
vm.runInContext('renderModelSettings()',ctx);assert.equal(nodes.modelChoice.value,'alternative');
vm.runInContext('applyModelChoice()',ctx).then(()=>{
 assert.equal(calls.length,1);assert.equal(calls[0].model,'alternative');assert.equal(calls[0].effort,'high');
 assert.equal(calls[0].expected_revision,0);assert.equal(calls[0].remember,true);
 console.log('PASS: selector options, dirty draft, explicit apply, stale tab, old queued setting, expired catalog and case switch');
});
