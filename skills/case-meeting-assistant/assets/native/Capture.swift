// Wisp Case Capture 0.2.1: derived from ppXD/Wisp v0.5.3
// WispSpeechSession's SpeechAnalyzer/AsyncStream/conversion flow is adapted under MIT.
// Official source retained separately; this fork adds reliable drain, timestamps and audio spool.
import Cocoa
import AVFoundation
import Speech
import CoreMedia
import Darwin
import CoreAudio

let fm = FileManager.default
let base = (Bundle.main.object(forInfoDictionaryKey: "CaseAssistantDataDirectory") as? String).map { URL(fileURLWithPath: $0) } ?? fm.homeDirectoryForCurrentUser.appendingPathComponent("Library/Application Support/CaseMeetingAssistantCommunity")
func atomic(_ value: Any, _ url: URL) throws {
    let data = try JSONSerialization.data(withJSONObject:value,options:[.sortedKeys])
    try data.write(to:url,options:.atomic)
    try? fm.setAttributes([.posixPermissions:0o600],ofItemAtPath:url.path)
}
func now() -> Double { Date().timeIntervalSince1970 }
func inputDevice() -> AudioDeviceID {
    var id=AudioDeviceID(0), size=UInt32(MemoryLayout<AudioDeviceID>.size)
    var a=AudioObjectPropertyAddress(mSelector:kAudioHardwarePropertyDefaultInputDevice,mScope:kAudioObjectPropertyScopeGlobal,mElement:kAudioObjectPropertyElementMain)
    if AudioObjectGetPropertyData(AudioObjectID(kAudioObjectSystemObject),&a,0,nil,&size,&id) != noErr {return 0};return id
}
func deviceLabel(_ id:AudioDeviceID)->String {
    var value:Unmanaged<CFString>?;var size=UInt32(MemoryLayout<Unmanaged<CFString>?>.size)
    var a=AudioObjectPropertyAddress(mSelector:kAudioObjectPropertyName,mScope:kAudioObjectPropertyScopeGlobal,mElement:kAudioObjectPropertyElementMain)
    if AudioObjectGetPropertyData(id,&a,0,nil,&size,&value) != noErr {return "设备ID \(id)（名称未取得）"};return value?.takeUnretainedValue() as String? ?? "未知设备"
}

@available(macOS 26.0, *)
final class Recorder {
    let session:String, matter:String, dir:URL, engine=AVAudioEngine()
    let transcriber:SpeechTranscriber, analyzer:SpeechAnalyzer
    let stream:AsyncStream<AnalyzerInput>, continuation:AsyncStream<AnalyzerInput>.Continuation
    let io=DispatchQueue(label:"wisp.case.audio.disk")
    let countLock=NSLock();var pending=0
    var target:AVAudioFormat!, converter:AVAudioConverter?, audio:AVAudioFile?
    var resultsTask:Task<Void,Error>?, log:FileHandle?
    var seq=0, revisions:[String:Int]=[:], last:[String:String]=[:]
    var sampleCount:Int64=0, chunkStart:Int64=0, chunk=0, audioRate:Double=0
    var running=false, stopping=false, error:String?, startedAt:Double=0, lastEventAt:Double=0
    var deviceName="未读取", framesDropped=0, fileName="", finalized=false
    var onStatus:(()->Void)?
    var deviceID:AudioDeviceID=0, defaultAtStart:AudioDeviceID=0
    var lastBufferAt:Double=0, firstBufferAt:Double=0, firstHostTime:UInt64=0, lastHostEnd:Double=0
    var rms:Double=0, audioClosed=false, asrDrained=false, stopTimedOut=false, tapInstalled=false
    var watchdogTriggered=false, replayTask:Task<Void,Error>?, sourceChannel="microphone"
    var replayPaths:[String]=[], replaySeconds:Int=0
    var startupPhase="未开始", startupAt:Double=0, startupCancelled=false, microphoneAuthorization = -1
    var limitSeconds=0, autoStopAt:Double=0
    var gaps:[[String:Any]]=[]
    init(session:String,matter:String) throws {
        self.session=session;self.matter=matter
        dir=base.appendingPathComponent("capture/\(session)")
        // A recording_id is a distinct invocation. Reusing a non-empty directory is refused.
        if fm.fileExists(atPath:dir.appendingPathComponent("events.jsonl").path) { throw NSError(domain:"WispCase",code:1,userInfo:[NSLocalizedDescriptionKey:"录音编号已存在；恢复时必须使用新录音段，不能覆盖旧段。"] ) }
        try fm.createDirectory(at:dir,withIntermediateDirectories:true,attributes:[.posixPermissions:0o700])
        fm.createFile(atPath:dir.appendingPathComponent("events.jsonl").path,contents:nil,attributes:[.posixPermissions:0o600])
        log=try FileHandle(forWritingTo:dir.appendingPathComponent("events.jsonl"))
        transcriber=SpeechTranscriber(locale:Locale(identifier:"zh-CN"),transcriptionOptions:[],reportingOptions:[.volatileResults,.fastResults],attributeOptions:[.audioTimeRange])
        analyzer=SpeechAnalyzer(modules:[transcriber],options:SpeechAnalyzer.Options(priority:.userInitiated,modelRetention:.lingering))
        (stream,continuation)=AsyncStream<AnalyzerInput>.makeStream()
    }
    func append(_ fields:[String:Any]) throws {
        seq += 1
        var d=fields;d["schema_version"]=1;d["event_seq"]=seq;d["session_id"]=session;d["matter_id"]=matter;d["recording_id"]=session;d["observed_at"]=now();d["speaker_id"]="unknown"
        var data=try JSONSerialization.data(withJSONObject:d,options:[.sortedKeys]);data.append(10)
        try log?.write(contentsOf:data);try log?.synchronize();lastEventAt=now()
    }
    func fault(_ message:String) {
        error=message;try? append(["event_type":"error","message":message]);onStatus?()
        if running && !stopping {Task {await stop()}}
    }
    func startupStep(_ name:String) throws {
        try Task.checkCancellation()
        guard !startupCancelled && !stopping && !finalized else {throw CancellationError()}
        startupPhase=name
        try io.sync {try append(["event_type":"startup_stage","stage":name,"microphone_authorization":microphoneAuthorization])}
        onStatus?()
    }
    func start() async throws {
        startupAt=now();try startupStep("检查麦克风权限")
        var permitted = !replayPaths.isEmpty
        if !permitted {
            let authorization=AVCaptureDevice.authorizationStatus(for:.audio)
            microphoneAuthorization=authorization.rawValue
            switch authorization {
            case .authorized: permitted=true
            case .notDetermined:
                try startupStep("等待麦克风授权")
                permitted=await AVCaptureDevice.requestAccess(for:.audio)
                microphoneAuthorization=AVCaptureDevice.authorizationStatus(for:.audio).rawValue
            default: permitted=false
            }
        }
        try startupStep("检查普通话模型")
        guard permitted else { throw NSError(domain:"WispCase",code:2,userInfo:[NSLocalizedDescriptionKey:"麦克风权限未允许。请在系统设置中允许案件沟通采集（公开版）。"] ) }
        if let request=try await AssetInventory.assetInstallationRequest(supporting:[transcriber]) {
            try startupStep("准备普通话模型");try await request.downloadAndInstall()
        }
        try startupStep("准备语音格式")
        target=await SpeechAnalyzer.bestAvailableAudioFormat(compatibleWith:[transcriber])
        try startupStep("启动语音分析器")
        guard target != nil else {throw NSError(domain:"WispCase",code:3,userInfo:[NSLocalizedDescriptionKey:"普通话语音模型未就绪"])}
        resultsTask=Task { [self] in
            for try await r in transcriber.results {
                let text=String(r.text.characters)
                let start=max(0,Int((CMTimeGetSeconds(r.range.start)*1000).rounded()))
                let end=max(start,Int((CMTimeGetSeconds(CMTimeRangeGetEnd(r.range))*1000).rounded()))
                io.async { [self] in
                    let id="MIC-\(start)";let signature=text+"|"+String(r.isFinal)
                    if last[id]==signature {return};last[id]=signature
                    revisions[id,default:0] += 1
                    do {try append(["event_type":revisions[id]==1 ? "segment":"revision","segment_id":id,"revision":revisions[id]!,"text":text,"is_final":r.isFinal,"audio_start_ms":start,"audio_end_ms":end,"source_channel":sourceChannel,"timing_basis":"SpeechTranscriber.range"])}
                    catch {fault("转写写盘失败：\(error.localizedDescription)")}
                }
            }
        }
        try await analyzer.start(inputSequence:stream)
        try startupStep("连接实际输入设备")
        if !replayPaths.isEmpty {try startReplay();return}
        let input=engine.inputNode
        defaultAtStart=inputDevice()
        guard defaultAtStart != 0 else {throw NSError(domain:"WispCase",code:6,userInfo:[NSLocalizedDescriptionKey:"系统没有有效输入设备"])}
        // Bind the actual I/O unit to the device selected by CoreAudio, not an AVCapture label.
        try input.auAudioUnit.setDeviceID(defaultAtStart)
        deviceID=input.auAudioUnit.deviceID
        let format=input.outputFormat(forBus:0)
        guard format.sampleRate > 0,format.channelCount>0 else {throw NSError(domain:"WispCase",code:4,userInfo:[NSLocalizedDescriptionKey:"没有可用麦克风输入格式"])}
        audioRate=format.sampleRate
        deviceName=deviceLabel(deviceID)
        startedAt=now()
        try append(["event_type":"started","language":"zh-CN","engine":"Wisp Apple Speech fork","microphone":deviceName,"actual_device_id":deviceID,"system_audio":false,"sample_rate":audioRate,"channels":format.channelCount,"recording_started_at":startedAt,"auto_stop_seconds":limitSeconds])
        tapInstalled=true
        input.installTap(onBus:0,bufferSize:2048,format:format) { [weak self] buffer,when in
            guard let self else{return}
            self.countLock.lock();self.pending += 1;let n=self.pending;self.countLock.unlock()
            if n>200 {
                self.countLock.lock();self.pending -= 1;self.framesDropped += 1;self.countLock.unlock()
                self.io.async {self.fault("音频写入积压溢出，发生采集缺口，不能报告完整。")};return
            }
            guard let copy=AVAudioPCMBuffer(pcmFormat:buffer.format,frameCapacity:buffer.frameLength) else{
                self.countLock.lock();self.pending -= 1;self.framesDropped += 1;self.countLock.unlock()
                self.io.async {self.fault("音频缓冲分配失败，发生采集缺口。")};return
            }
            copy.frameLength=buffer.frameLength
            let src=UnsafeMutableAudioBufferListPointer(UnsafeMutablePointer(mutating:buffer.audioBufferList))
            let dst=UnsafeMutableAudioBufferListPointer(copy.mutableAudioBufferList)
            for i in 0..<src.count {if let a=src[i].mData,let b=dst[i].mData {memcpy(b,a,Int(src[i].mDataByteSize))}}
            self.io.async {
                defer {self.countLock.lock();self.pending -= 1;self.countLock.unlock()}
                do {
                    let host=when.isHostTimeValid ? AVAudioTime.seconds(forHostTime:when.hostTime) : 0
                    if self.firstBufferAt==0 {
                        self.firstBufferAt=now()-Double(copy.frameLength)/copy.format.sampleRate;self.firstHostTime=when.hostTime
                        try self.append(["event_type":"input_confirmed","actual_device_id":self.deviceID,"actual_device_name":self.deviceName,"first_buffer_wall_time":self.firstBufferAt,"first_host_time":when.hostTime,"time_basis":"AVAudioEngine tap hostTime; wall time estimated at first callback"])
                    }
                    if host>0 && self.lastHostEnd>0 && host-self.lastHostEnd>0.15 {
                        let gap:[String:Any]=["from_host":self.lastHostEnd,"to_host":host,"seconds":host-self.lastHostEnd]
                        self.gaps.append(gap);try self.append(["event_type":"audio_gap","gap":gap])
                    }
                    if host>0 {self.lastHostEnd=host+Double(copy.frameLength)/copy.format.sampleRate}
                    self.lastBufferAt=now();try self.consume(copy)
                } catch {self.fault("音频处理失败：\(error.localizedDescription)")}
            }
        }
        engine.prepare();try engine.start();deviceID=input.auAudioUnit.deviceID;deviceName=deviceLabel(deviceID);running=true;startupPhase="已启动，核对采样";onStatus?()
    }
    func consume(_ buffer:AVAudioPCMBuffer) throws {
        if audio==nil {
            chunk += 1;chunkStart=sampleCount;fileName=String(format:"audio-%05d.wav",chunk)
            audio=try AVAudioFile(forWriting:dir.appendingPathComponent(fileName),settings:buffer.format.settings,commonFormat:buffer.format.commonFormat,interleaved:buffer.format.isInterleaved)
        }
        try audio!.write(from:buffer);sampleCount += Int64(buffer.frameLength)
        if let ptr=buffer.floatChannelData?[0],buffer.frameLength>0 {
            var sum:Double=0;for i in stride(from:0,to:Int(buffer.frameLength),by:16){let v=Double(ptr[i]);sum += v*v};rms=sqrt(sum/Double(max(1,Int(buffer.frameLength)/16)))
        }
        // Convert using the official Wisp engine flow, keeping the continuous sample timeline.
        if converter==nil {converter=AVAudioConverter(from:buffer.format,to:target)}
        guard let converter else {throw NSError(domain:"WispCase",code:5,userInfo:[NSLocalizedDescriptionKey:"语音采样率转换失败"])}
        let cap=AVAudioFrameCount(Double(buffer.frameLength)*target.sampleRate/buffer.format.sampleRate)+32
        guard let out=AVAudioPCMBuffer(pcmFormat:target,frameCapacity:cap) else{return}
        var used=false;var err:NSError?
        converter.convert(to:out,error:&err) {_,status in
            if used {status.pointee = .noDataNow;return nil}
            used=true;status.pointee = .haveData;return buffer
        }
        if let err {throw err};continuation.yield(AnalyzerInput(buffer:out))
        if Double(sampleCount-chunkStart)/audioRate>=10 {try closeChunk()}
    }
    func closeChunk() throws {
        guard audio != nil else{return};audio=nil
        let url=dir.appendingPathComponent(fileName)
        let handle=try FileHandle(forWritingTo:url);try handle.synchronize();try handle.close()
        try append(["event_type":"audio_closed","file":fileName,"audio_start_ms":Int(Double(chunkStart)*1000/audioRate),"audio_end_ms":Int(Double(sampleCount)*1000/audioRate),"sample_rate":audioRate,"bytes":(try fm.attributesOfItem(atPath:url.path)[.size] as? NSNumber)?.int64Value ?? 0])
    }
    func startReplay() throws {
        sourceChannel="saved_audio_replay_test";deviceName="已保存虚构音频实时回放测试（未启用麦克风）"
        let f=try AVAudioFile(forReading:URL(fileURLWithPath:replayPaths[0]));audioRate=f.processingFormat.sampleRate
        startedAt=now();firstBufferAt=startedAt;running=true
        try append(["event_type":"started","language":"zh-CN","source_channel":sourceChannel,"microphone":deviceName,"system_audio":false,"sample_rate":audioRate,"recording_started_at":startedAt,"replay_test":true])
        replayTask=Task { [self] in
            var cycle=0
            while running && now()-startedAt<Double(replaySeconds) {
                let cycleStart=now();cycle += 1
                for path in replayPaths {
                    let f=try AVAudioFile(forReading:URL(fileURLWithPath:path))
                    while running && f.framePosition<f.length && now()-startedAt<Double(replaySeconds) {
                        let buffer=AVAudioPCMBuffer(pcmFormat:f.processingFormat,frameCapacity:AVAudioFrameCount(audioRate/10))!;try f.read(into:buffer)
                        try io.sync {lastBufferAt=now();try consume(buffer)}
                        let due=startedAt+Double(sampleCount)/audioRate;let wait=max(0,due-now())
                        if wait>0 {try await Task.sleep(nanoseconds:UInt64(wait*1e9))}
                    }
                }
                // Real-time silence between samples; the entire pipeline stays active.
                let until=min(startedAt+Double(replaySeconds),cycleStart+90)
                while running && now()<until {
                    let format=f.processingFormat;let buf=AVAudioPCMBuffer(pcmFormat:format,frameCapacity:AVAudioFrameCount(audioRate/10))!;buf.frameLength=buf.frameCapacity
                    for b in UnsafeMutableAudioBufferListPointer(buf.mutableAudioBufferList){if let p=b.mData {memset(p,0,Int(b.mDataByteSize))}}
                    try io.sync {lastBufferAt=now();try consume(buf)}
                    let wait=max(0,startedAt+Double(sampleCount)/audioRate-now());if wait>0 {try await Task.sleep(nanoseconds:UInt64(wait*1e9))}
                }
            }
            if running {await stop()}
        }
    }
    func watchdog() {
        if !running && !stopping && !finalized && !startupCancelled && startupAt>0 && now()-startupAt>10 {
            startupCancelled=true
            io.sync {fault("采集启动超过10秒，停在“\(startupPhase)”；本次未开始录音。请检查此步骤后重新开始，程序不会延迟开麦。")}
            Task {await stop()};return
        }
        guard running && !stopping && !watchdogTriggered else{return}
        let snap=io.sync { (sampleCount,lastBufferAt) }
        var problem:String?
        if now()-startedAt>6 && snap.0==0 {problem="未收到任何实际音频采样；本段没有成功录音。请检查输入设备后重新开始。"}
        else if snap.1>0 && now()-snap.1>4 {problem="实际音频输入已中断超过4秒；已保存部分保留，请重新开始新录音段。"}
        else if sourceChannel=="microphone" && inputDevice() != defaultAtStart {problem="系统输入设备发生变化；为避免来源混淆，本段将停止。核对新设备后继续录音。"}
        if let problem {watchdogTriggered=true;io.sync {fault(problem)};Task {await stop()}}
    }
    func stop() async {
        guard !stopping && !finalized else{return};startupCancelled=true;stopping=true;onStatus?()
        if tapInstalled {engine.inputNode.removeTap(onBus:0);tapInstalled=false}
        engine.stop();engine.reset();running=false
        await withCheckedContinuation { (c:CheckedContinuation<Void,Never>) in io.async {c.resume()} }
        continuation.finish()
        if sampleCount==0 {
            io.sync {if error==nil {fault("没有音频采样，录音失败；未保存原音频。")}}
            await analyzer.cancelAndFinishNow();resultsTask?.cancel()
        } else {
            let watchdog=Task { [self] in
                try? await Task.sleep(nanoseconds:8_000_000_000)
                if !Task.isCancelled && !asrDrained {stopTimedOut=true;await analyzer.cancelAndFinishNow();resultsTask?.cancel()}
            }
            do {try await analyzer.finalizeAndFinishThroughEndOfInput();try await resultsTask?.value;asrDrained = !stopTimedOut}
            catch {io.sync {fault("识别收尾失败：\(error.localizedDescription)")}}
            watchdog.cancel()
            if stopTimedOut {io.sync {fault("语音识别收尾超过8秒，已停止等待；原音频保存，末尾转写可能缺失，需复核。")}}
        }
        io.sync {
            do {
                try closeChunk();audioClosed=true;finalized=true
                try append(["event_type":"stopped","tail_drained":asrDrained,"audio_closed":audioClosed,"capture_stopped":true,"audio_samples":sampleCount,"audio_duration_ms":Int(Double(sampleCount)*1000/max(1,audioRate)),"frames_dropped":framesDropped,"gaps":gaps,"error":error as Any? ?? NSNull()])
                try log?.synchronize();try log?.close();log=nil
            } catch {self.error="结束保存失败：\(error.localizedDescription)"}
        }
        stopping=false;onStatus?()
    }
    func status()->[String:Any] {
        return io.sync {
            var d:[String:Any]=["recording_id":session,"matter_id":matter,"running":running,"stopping":stopping,"tail_drained":asrDrained,"audio_closed":audioClosed,"capture_stopped":finalized,"input_confirmed":sampleCount>0,"actual_device_id":deviceID,"default_device_id":defaultAtStart,"first_buffer_at":firstBufferAt,"last_buffer_at":lastBufferAt,"input_level":rms,"source_channel":sourceChannel,"startup_phase":startupPhase,"startup_cancelled":startupCancelled,"startup_at":startupAt,"microphone_authorization":microphoneAuthorization,"gaps":gaps,"error":error as Any? ?? NSNull(),"event_seq":seq,"language":"zh-CN","engine":"Wisp Apple Speech fork 0.2.1","microphone":deviceName,"system_audio":false,"started_at":startedAt,"last_event_at":lastEventAt,"audio_samples":sampleCount,"audio_duration_ms":Int(Double(sampleCount)*1000/max(1,audioRate)),"frames_dropped":framesDropped,"spool":dir.path]
            d["limit_seconds"]=limitSeconds;d["auto_stop_at"]=autoStopAt
            d["remaining_seconds"]=running && autoStopAt>0 ? max(0,Int(ceil(autoStopAt-now()))) : 0
            return d
        }
    }
}

@available(macOS 26.0, *)
final class AppDelegate:NSObject,NSApplicationDelegate {
    var recorder:Recorder?, window:NSWindow!, label:NSTextField!, timer:Timer?, lastCommand="", busy=false, lastError:String?, lockFD:Int32 = -1
    func applicationDidFinishLaunching(_ notification:Notification) {
        try? fm.createDirectory(at:base,withIntermediateDirectories:true,attributes:[.posixPermissions:0o700])
        lockFD=open(base.appendingPathComponent("capture.lock").path,O_CREAT|O_RDWR,0o600)
        guard flock(lockFD,LOCK_EX|LOCK_NB)==0 else {NSApp.terminate(nil);return}
        let menu=NSMenu(),item=NSMenuItem(),appMenu=NSMenu()
        menu.addItem(item);item.submenu=appMenu
        appMenu.addItem(NSMenuItem(title:"退出 案件沟通采集（公开版）",action:#selector(NSApplication.terminate(_:)),keyEquivalent:"q"));NSApp.mainMenu=menu
        window=NSWindow(contentRect:NSRect(x:0,y:0,width:560,height:240),styleMask:[.titled,.closable,.miniaturizable],backing:.buffered,defer:false)
        window.title="案件沟通采集（公开版） · 0.2.1 · 独立副本";window.center()
        label=NSTextField(wrappingLabelWithString:"等待本地案件页面启动。\n普通话 · 系统默认麦克风 · 系统音频关闭\n官方 Wisp 保留，本应用只负责录音转写。")
        label.frame=NSRect(x:24,y:65,width:510,height:140);window.contentView?.addSubview(label)
        let button=NSButton(title:"停止录音并保存尾段",target:self,action:#selector(stopButton));button.frame=NSRect(x:24,y:20,width:260,height:35);window.contentView?.addSubview(button)
        window.makeKeyAndOrderFront(nil);NSApp.activate(ignoringOtherApps:true)
        // Never replay a stale start command on launch. Caller must issue a fresh command.
        if let d=try? Data(contentsOf:base.appendingPathComponent("capture-control.json")),let c=(try? JSONSerialization.jsonObject(with:d)) as? [String:Any] {lastCommand=c["command_id"] as? String ?? ""}
        timer=Timer.scheduledTimer(withTimeInterval:0.3,repeats:true){[weak self] _ in self?.poll();self?.recorder?.watchdog();self?.publish()}
    }
    @objc func stopButton(){guard let r=recorder else{return};Task {await r.stop()}}
    func publish(){
        if recorder?.startupCancelled == true && recorder?.finalized == true {busy=false}
        var d=recorder?.status() ?? ["running":false,"language":"zh-CN","system_audio":false]
        d["heartbeat"]=now();d["pid"]=getpid();d["busy"]=busy;d["command_id"]=lastCommand
        if let lastError {d["control_error"]=lastError}
        try? atomic(d,base.appendingPathComponent("capture-status.json"))
        let state=(d["stopping"] as? Bool==true) ? "正在等待尾段" : ((d["running"] as? Bool==true) ? "正在录音和转写":(busy ? "正在启动：\(d["startup_phase"] ?? "准备中")":"未录音"))
        label.stringValue="\(state)\n普通话 · \(d["microphone"] ?? "系统默认麦克风") · 系统音频关闭\n会话：\(d["recording_id"] ?? "未绑定")\n\(lastError ?? (d["error"] as? String ?? "由本地案件页面控制；说话人身份未知。"))"
    }
    func poll(){
        guard let data=try? Data(contentsOf:base.appendingPathComponent("capture-control.json")),let c=(try? JSONSerialization.jsonObject(with:data)) as? [String:Any],let id=c["command_id"] as? String,id != lastCommand else{return}
        if busy && c["action"] as? String != "stop" {return}
        lastCommand=id;lastError=nil
        switch c["action"] as? String {
        case "choose_case","choose_pack":
            busy=true;let panel=NSOpenPanel();panel.canChooseDirectories=true;panel.canChooseFiles=(c["action"] as? String=="choose_pack");panel.allowsMultipleSelection=false;panel.message="选择本机已下载的案件目录或会前交接包"
            panel.begin {result in
                try? atomic(["command_id":id,"path":result == .OK ? (panel.url?.path ?? "") : ""],base.appendingPathComponent("selection.json"));self.busy=false
            }
        case "start":
            guard recorder?.running != true,recorder?.stopping != true else {lastError="已有录音正在运行";return}
            guard let sid=c["recording_id"] as? String,sid.range(of:"^[A-Za-z0-9_-]{1,100}$",options:.regularExpression) != nil,let mid=c["matter_id"] as? String else {lastError="录音身份无效";return}
            busy=true
            Task {do {
                recorder=nil
                let r=try Recorder(session:sid,matter:mid)
                if let paths=c["replay_paths"] as? [String],!paths.isEmpty {
                    let prefix=base.appendingPathComponent("verification/round2/replay-input").path+"/"
                    guard mid.hasPrefix("demo") && paths.allSatisfy({URL(fileURLWithPath:$0).resolvingSymlinksInPath().path.hasPrefix(prefix)}) else {throw NSError(domain:"WispCase",code:7,userInfo:[NSLocalizedDescriptionKey:"回放测试仅允许指定虚构录音样本"])}
                    r.replayPaths=paths;r.replaySeconds=min(3900,max(30,c["replay_seconds"] as? Int ?? 90))
                }
                r.limitSeconds=c["max_seconds"] as? Int ?? 0
                recorder=r;try await r.start()
                if let seconds=c["max_seconds"] as? Int,seconds>0 {
                    r.autoStopAt=now()+Double(seconds)
                    // Test-only safety timer lives in the recorder process, so a
                    // disconnected analysis service cannot extend the test.
                    Task {try? await Task.sleep(nanoseconds:UInt64(seconds)*1_000_000_000);if self.recorder === r && r.running {await r.stop()}}
                }
            }catch{if lastCommand==id {lastError=recorder?.error ?? error.localizedDescription}}
                if lastCommand==id {busy=false;publish()}}
        case "stop":guard let r=recorder else{return};busy=true;Task {await r.stop();busy=false;publish()}
        default:lastError="未知采集命令"
        }
    }
    func applicationShouldTerminate(_ sender:NSApplication)->NSApplication.TerminateReply {
        if let r=recorder,r.running || r.stopping || busy {Task {await r.stop();busy=false;publish();NSApp.reply(toApplicationShouldTerminate:true)};return .terminateLater}
        publish();return .terminateNow
    }
    func applicationShouldTerminateAfterLastWindowClosed(_ sender:NSApplication)->Bool {true}
}
if #available(macOS 26.0,*) {let app=NSApplication.shared;let delegate=AppDelegate();app.delegate=delegate;app.setActivationPolicy(.regular);app.run()}
