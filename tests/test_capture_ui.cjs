const fs = require('fs');
const vm = require('vm');
const assert = require('assert');
const html = fs.readFileSync(__dirname + '/../skills/case-meeting-assistant/assets/runtime/index.html', 'utf8');
const source = html.slice(html.indexOf('function render(){'), html.indexOf('let refreshing=false;'));
function view(capture, extra = {}) {
  const elements = {};
  const ctx = {
    state: {session:'new-session', phase:'ready', capture_connected:true,
      capture_for_session:true, capture, jobs:[], ...extra},
    seenSession:null, lastCards:'', lastTranscript:'', lastQA:'', lastAudio:JSON.stringify(extra.audio||[]), selectionId:'',
    $: id => elements[id] ||= {textContent:'', style:{}, dataset:{}, options:[]},
    renderCards(){}, renderTranscript(){}, renderQA(){}, renderModelSettings(){}, renderSessionView(){}, effortLabel(v){return v}
  };
  vm.runInNewContext(source + ';render()', ctx);
  return elements;
}
let e = view({running:true, error:'old recording error', microphone:'other-device'}, {capture_for_session:false});
assert.equal(e.capture.textContent, '本次尚未录音');
assert.equal(e.error.textContent, '');
assert(!e.settings.textContent.includes('other-device'));
e = view({busy:true, startup_phase:'等待麦克风授权'});
assert.equal(e.capture.textContent, '正在启动：等待麦克风授权');
assert(e.startRecording.disabled && e.testRecording.disabled);
e = view({busy:true, stopping:true});
assert.equal(e.capture.textContent, '正在封闭尾段');
e = view({running:true, input_confirmed:true, limit_seconds:45, remaining_seconds:30});
assert(e.recordingMode.textContent.includes('剩余约30秒'));
assert(e.startRecording.disabled && e.testRecording.disabled);
e = view({running:true, input_confirmed:true, limit_seconds:0});
assert(e.recordingMode.textContent.includes('没有自动停止计时'));
e = view({running:false, audio_closed:true});
assert.equal(e.capture.textContent, '原音频已封闭');
assert(e.recordingMode.textContent.includes('已停止并保存'));
assert(!e.recordingMode.textContent.includes('尚未录音'));
assert(!e.startRecording.disabled && !e.testRecording.disabled);
e = view({}, {capture_for_session:false,audio:[{recording_id:'old-R01',file:'audio.wav',audio_start_ms:0,audio_end_ms:1}]});
assert(e.capture.textContent.includes('已有原音频保留'));
e = view({running:true, source_channel:'saved_audio_replay_test', limit_seconds:0});
assert(e.recordingMode.textContent.includes('麦克风关闭'));
assert(e.recordingMode.textContent.includes('自动停止'));
assert(!e.recordingMode.textContent.includes('没有自动停止'));
console.log('PASS: eight capture/session/startup/stop/countdown/replay UI cases');
