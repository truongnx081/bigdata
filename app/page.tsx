'use client';

import { useEffect, useRef, useState, type ComponentType, type SyntheticEvent } from 'react';
import {
  Activity,
  BrainCircuit,
  CarFront,
  CheckCircle2,
  Clock3,
  FileVideo,
  Gauge,
  LoaderCircle,
  MapPin,
  Radio,
  ScanLine,
  Square,
  TriangleAlert,
  Upload,
  Video,
  WifiOff,
} from 'lucide-react';
import { CartesianGrid, Line, LineChart, XAxis, YAxis } from 'recharts';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Checkbox } from '@/components/ui/checkbox';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from '@/components/ui/chart';

const API_BASE = process.env.NEXT_PUBLIC_VIDEO_API_URL || 'http://localhost:8102';

type TrafficEvent = {
  event_id?: string;
  timestamp?: string;
  speed_kmh?: number;
  density_pct?: number;
  occupancy_pct?: number;
  vehicle_count?: number;
  quality?: number;
  inference_ms?: number;
  vehicles?: VehicleDetail[];
  video_frame?: number;
};

type VehicleDetail = {
  track_id: number;
  vehicle_type: string;
  speed_kmh?: number | null;
  confidence: number;
  bbox: number[];
};

type Analysis = TrafficEvent & {
  prediction_label?: string;
  congestion_level?: 'smooth' | 'moderate' | 'heavy' | 'critical';
  risk_score?: number;
  anomaly?: boolean;
  anomaly_score?: number;
  alert_title?: string | null;
  processed_at?: string;
};

type VideoStatus = {
  active: boolean;
  loading: boolean;
  kafka_connected: boolean;
  source_ref?: string | null;
  road_name?: string | null;
  latitude: number;
  longitude: number;
  fps: number;
  processing_fps: number;
  dropped_frames: number;
  detector?: string;
  detector_device?: string;
  started_at?: string | null;
  frame_index: number;
  total_events: number;
  last_error?: string | null;
  live_metrics?: TrafficEvent | null;
  latest_event?: TrafficEvent | null;
  analysis?: Analysis | null;
};

type HistoryPoint = { time: string; speed: number; density: number; risk: number };

const chartConfig = {
  speed: { label: 'Tốc độ (km/h)', color: '#22d3ee' },
  density: { label: 'Mật độ (%)', color: '#f59e0b' },
  risk: { label: 'Rủi ro', color: '#fb7185' },
} satisfies ChartConfig;

const chartLegend = [
  { label: 'Tốc độ trung bình', unit: 'km/h', color: '#22d3ee', dashed: false },
  { label: 'Mật độ giao thông', unit: '%', color: '#f59e0b', dashed: false },
  { label: 'Điểm rủi ro', unit: '/100', color: '#fb7185', dashed: true },
];

const levels = {
  smooth: { label: 'Lưu thông ổn định', color: 'text-emerald-700', badge: 'bg-emerald-50 border-emerald-200' },
  moderate: { label: 'Mật độ đang tăng', color: 'text-amber-700', badge: 'bg-amber-50 border-amber-200' },
  heavy: { label: 'Có khả năng ùn tắc', color: 'text-orange-700', badge: 'bg-orange-50 border-orange-200' },
  critical: { label: 'Nguy cơ ùn tắc nghiêm trọng', color: 'text-rose-700', badge: 'bg-rose-50 border-rose-200' },
} as const;

function number(value: number | undefined, digits = 0) {
  return typeof value === 'number' && Number.isFinite(value) ? value.toFixed(digits) : '—';
}

function time(value?: string) {
  if (!value) return 'Chưa có dữ liệu';
  return new Intl.DateTimeFormat('vi-VN', { hour: '2-digit', minute: '2-digit', second: '2-digit' }).format(new Date(value));
}

async function responseError(response: Response) {
  try {
    const payload = (await response.json()) as { detail?: string | Array<{ msg?: string }> };
    if (typeof payload.detail === 'string') return payload.detail;
    if (Array.isArray(payload.detail)) return payload.detail.map((item) => item.msg).filter(Boolean).join(', ');
  } catch {
    // The fallback below is clearer than exposing an invalid JSON response.
  }
  return `Không thể cập nhật nguồn video (${response.status})`;
}

function MetricCard({
  label,
  value,
  unit,
  icon: Icon,
  tone,
}: {
  label: string;
  value: string;
  unit: string;
  icon: ComponentType<{ className?: string }>;
  tone: 'cyan' | 'amber' | 'violet' | 'rose';
}) {
  return (
    <Card className="metric-card border-slate-200/80 bg-white shadow-sm" data-tone={tone}>
      <CardHeader>
        <div className="flex items-start justify-between gap-4">
          <CardDescription className="text-slate-500">{label}</CardDescription>
          <span className="metric-icon grid size-9 place-items-center rounded-lg"><Icon className="size-4" /></span>
        </div>
      </CardHeader>
      <CardContent className="mt-auto">
        <p className="font-mono text-3xl font-semibold tracking-tight text-[#123252] sm:text-4xl">
          {value}<span className="ml-2 text-xs font-normal text-slate-500">{unit}</span>
        </p>
      </CardContent>
    </Card>
  );
}

export default function Home() {
  const [status, setStatus] = useState<VideoStatus | null>(null);
  const [connected, setConnected] = useState(false);
  const [history, setHistory] = useState<HistoryPoint[]>([]);
  const [videoFile, setVideoFile] = useState<File | null>(null);
  const [loopVideo, setLoopVideo] = useState(true);
  const [sourceBusy, setSourceBusy] = useState(false);
  const [sourceFeedback, setSourceFeedback] = useState<{ tone: 'success' | 'error'; text: string } | null>(null);
  const lastEvent = useRef<string | null>(null);

  useEffect(() => {
    let mounted = true;
    const poll = async () => {
      try {
        const response = await fetch(`${API_BASE}/api/status`, { cache: 'no-store' });
        if (!response.ok) throw new Error('Dịch vụ video chưa sẵn sàng');
        const data = (await response.json()) as VideoStatus;
        if (!mounted) return;
        setStatus(data);
        setConnected(true);
        const sample = data.live_metrics ?? data.latest_event ?? data.analysis;
        const sampleId = sample?.event_id ?? sample?.timestamp ?? null;
        if (sample && sampleId && sampleId !== lastEvent.current) {
          lastEvent.current = sampleId;
          setHistory((items) => [
            ...items,
            {
              time: time(sample.timestamp),
              speed: sample.speed_kmh ?? 0,
              density: sample.density_pct ?? 0,
              risk: data.analysis?.risk_score ?? 0,
            },
          ].slice(-32));
        }
      } catch {
        if (mounted) setConnected(false);
      }
    };
    void poll();
    const interval = window.setInterval(poll, 1000);
    return () => { mounted = false; window.clearInterval(interval); };
  }, []);

  const visibleFeedback = sourceFeedback ?? (
    status?.last_error ? { tone: 'error' as const, text: status.last_error } : null
  );

  const startFileSource = async (event: SyntheticEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!videoFile) return;
    setSourceBusy(true);
    setSourceFeedback(null);
    try {
      const query = new URLSearchParams({ filename: videoFile.name, loop: String(loopVideo) });
      const response = await fetch(`${API_BASE}/api/source/upload?${query}`, {
        method: 'POST',
        headers: { 'Content-Type': videoFile.type || 'application/octet-stream' },
        body: videoFile,
      });
      if (!response.ok) throw new Error(await responseError(response));
      setHistory([]);
      lastEvent.current = null;
      setSourceFeedback({ tone: 'success', text: `Đã tải ${videoFile.name}. Hệ thống đang phân tích video.` });
    } catch (error) {
      setSourceFeedback({ tone: 'error', text: error instanceof Error ? error.message : 'Không thể tải video.' });
    } finally {
      setSourceBusy(false);
    }
  };

  const stopSource = async () => {
    setSourceBusy(true);
    setSourceFeedback(null);
    try {
      const response = await fetch(`${API_BASE}/api/source/stop`, { method: 'POST' });
      if (!response.ok) throw new Error(await responseError(response));
      setSourceFeedback({ tone: 'success', text: 'Đã dừng nguồn video.' });
    } catch (error) {
      setSourceFeedback({ tone: 'error', text: error instanceof Error ? error.message : 'Không thể dừng nguồn video.' });
    } finally {
      setSourceBusy(false);
    }
  };

  const event = status?.latest_event;
  const liveMetrics = status?.live_metrics ?? event;
  const analysis = status?.analysis;
  const vehicles = liveMetrics?.vehicles ?? [];
  const liveSpeed = liveMetrics?.speed_kmh;
  const liveDensity = liveMetrics?.density_pct;
  const liveVehicleCount = liveMetrics?.vehicle_count;
  const level = analysis?.congestion_level ? levels[analysis.congestion_level] : null;
  const isLive = Boolean(connected && status?.active);
  const isLoading = Boolean(connected && status?.loading);
  const pipeline = [
    { label: 'Nguồn video', caption: status?.source_ref || 'Chờ chọn video', icon: Video, ready: isLive },
    { label: 'Gửi dữ liệu', caption: 'Tốc độ, vị trí, thời gian', icon: Radio, ready: Boolean(status?.kafka_connected) },
    { label: 'Phân tích liên tục', caption: 'Đánh giá mật độ và tốc độ', icon: ScanLine, ready: Boolean(event) },
    { label: 'Cảnh báo ùn tắc', caption: 'Kết quả hiển thị tức thời', icon: BrainCircuit, ready: Boolean(analysis) },
  ];

  return (
    <main className="min-h-screen bg-background text-foreground">
      <header className="sticky top-0 z-40 border-b border-slate-200/80 bg-white/88 backdrop-blur-xl">
        <div className="mx-auto flex max-w-[1540px] items-center justify-between px-5 py-4 lg:px-8">
          <div className="flex items-center gap-3">
            <span className="grid size-10 place-items-center rounded-xl border border-cyan-200 bg-cyan-50 text-cyan-700">
              <ScanLine className="size-5" />
            </span>
            <div>
              <p className="text-sm font-semibold tracking-[0.18em] text-cyan-700">LIVEROAD</p>
              <p className="text-xs text-slate-500">Bảng theo dõi giao thông</p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <Badge className={isLive ? 'border border-emerald-200 bg-emerald-50 text-emerald-700' : 'border border-amber-200 bg-amber-50 text-amber-700'}>
              {isLoading ? <LoaderCircle className="size-3 animate-spin" /> : <span className={`size-1.5 rounded-full ${isLive ? 'animate-pulse bg-emerald-500' : 'bg-amber-500'}`} />}
              {isLive ? 'Đang phân tích thời gian thực' : isLoading ? 'Đang mở nguồn video' : connected ? 'Chờ chọn nguồn' : 'Chờ dịch vụ video'}
            </Badge>
            <Badge variant="outline" className="hidden border-slate-200 text-slate-600 sm:inline-flex">
              {status?.kafka_connected ? 'Dữ liệu đã kết nối' : 'Chờ dữ liệu'}
            </Badge>
          </div>
        </div>
      </header>

      <div className="mx-auto max-w-[1540px] space-y-5 px-5 py-6 lg:px-8">
        {!connected && (
          <div className="flex items-center gap-3 rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
            <WifiOff className="size-4 shrink-0 text-amber-600" />
            Website đã sẵn sàng. Hãy chạy dịch vụ video để có thể chọn nguồn ngay trên web.
          </div>
        )}

        <Card className="border-cyan-100 bg-white shadow-sm">
          <CardHeader className="border-b border-slate-100">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div>
                <CardTitle className="flex items-center gap-2 text-[#123252]"><Video className="size-4 text-cyan-700" /> Chọn nguồn dữ liệu video</CardTitle>
                <CardDescription className="mt-1 text-slate-500">Chọn video trên máy để bắt đầu phân tích.</CardDescription>
              </div>
              {(isLive || isLoading) && (
                <Button type="button" variant="destructive" size="sm" onClick={stopSource} disabled={sourceBusy}>
                  <Square className="size-3.5 fill-current" /> Dừng nguồn
                </Button>
              )}
            </div>
          </CardHeader>
          <CardContent className="grid gap-5 pt-1 lg:grid-cols-[minmax(0,1.4fr)_minmax(280px,.6fr)]">
            <div className="space-y-4">
              <form onSubmit={startFileSource} className="space-y-3">
                <label htmlFor="video-file" className="block text-xs font-medium uppercase tracking-[0.12em] text-slate-500">Chọn video từ máy tính</label>
                <div className="flex flex-col gap-2 sm:flex-row">
                  <Input
                    id="video-file"
                    type="file"
                    accept="video/mp4,video/webm,video/quicktime,video/x-msvideo,video/x-matroska,.m4v,.mpeg,.mpg"
                    onChange={(event) => setVideoFile(event.target.files?.[0] ?? null)}
                    className="h-10 border-slate-200 bg-white pt-1.5 text-slate-700 file:text-cyan-700"
                    disabled={!connected || sourceBusy}
                    required
                  />
                  <Button type="submit" size="lg" className="h-10 min-w-32" disabled={!connected || sourceBusy || !videoFile}>
                    {sourceBusy ? <LoaderCircle className="animate-spin" /> : <Upload />} Tải & chạy
                  </Button>
                </div>
                <p className="text-xs text-slate-600">Tạm thời chỉ dùng video tải lên. Hỗ trợ MP4, MOV, AVI, MKV, WebM, M4V, MPEG; tối đa 2 GB.</p>
              </form>
              <div className="flex w-fit items-center gap-2 text-sm text-slate-600">
                <Checkbox id="loop-video" checked={loopVideo} onCheckedChange={(checked) => setLoopVideo(checked === true)} />
                <label htmlFor="loop-video" className="cursor-pointer">
                Tự phát lại khi video kết thúc
                </label>
              </div>
              {visibleFeedback && (
                <p aria-live="polite" className={`text-sm ${visibleFeedback.tone === 'success' ? 'text-emerald-700' : 'text-rose-700'}`}>
                  {visibleFeedback.text}
                </p>
              )}
            </div>

            <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
              <p className="text-xs font-medium uppercase tracking-[0.12em] text-slate-500">Nguồn hiện tại</p>
              <div className="mt-3 flex items-start gap-3">
                <span className={`grid size-9 shrink-0 place-items-center rounded-lg ${isLive ? 'bg-emerald-50 text-emerald-700' : isLoading ? 'bg-amber-50 text-amber-700' : 'bg-white text-slate-500'}`}>
                  {isLoading ? <LoaderCircle className="size-4 animate-spin" /> : <FileVideo className="size-4" />}
                </span>
                <div className="min-w-0">
                  <p className="truncate text-sm font-medium text-[#123252]" title={status?.source_ref || undefined}>{status?.source_ref || 'Chưa chọn nguồn'}</p>
                  <p className="mt-1 text-xs text-slate-500">{isLive ? `Đang phân tích ${number(status?.processing_fps, 1)} hình/giây` : isLoading ? 'Đang chuẩn bị video...' : status?.last_error || 'Chọn video để bắt đầu'}</p>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        <section className="grid gap-5 xl:grid-cols-[minmax(0,1.55fr)_minmax(360px,.75fr)]">
          <Card className="border-slate-200 bg-white py-0 shadow-sm">
            <CardHeader className="flex-row items-center justify-between border-b border-slate-100 py-4">
              <div>
                <CardTitle className="flex items-center gap-2 text-[#123252]">
                  <Radio className={`size-4 ${isLive ? 'text-emerald-600' : 'text-cyan-700'}`} /> Hình ảnh giao thông trực tiếp
                </CardTitle>
                <CardDescription className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-slate-500">
                  <span className="flex items-center gap-1"><MapPin className="size-3" /> {status?.road_name || 'Nguồn video trực tiếp'}</span>
                  <span className="flex items-center gap-1"><Clock3 className="size-3" /> {time(liveMetrics?.timestamp)}</span>
                </CardDescription>
              </div>
              <span className="font-mono text-xs text-slate-500">Khung hình {status?.frame_index ?? '—'}</span>
            </CardHeader>
            <CardContent className="p-0">
              <div className="video-grid relative grid min-h-[430px] place-items-center overflow-hidden bg-slate-100 lg:min-h-[570px]">
                {isLive ? (
                  <img
                    key={status?.started_at || status?.source_ref}
                    src={`${API_BASE}/api/stream.mjpg?session=${encodeURIComponent(status?.started_at || '')}`}
                    alt="Luồng giao thông đang được phân tích theo thời gian thực"
                    className="absolute inset-0 size-full object-contain"
                  />
                ) : (
                  <div className="relative z-10 max-w-md px-8 text-center">
                    <span className="mx-auto mb-5 grid size-16 place-items-center rounded-full border border-cyan-200 bg-cyan-50 text-cyan-700">
                      <Video className="size-7" />
                    </span>
                    <h1 className="text-xl font-semibold text-[#123252]">Đang chờ terminal video</h1>
                    <p className="mt-2 text-sm leading-6 text-slate-500">Hình ảnh phân tích sẽ xuất hiện khi nguồn video được khởi chạy.</p>
                  </div>
                )}
                <div className="pointer-events-none absolute inset-x-0 top-1/2 h-px bg-cyan-300/20 shadow-[0_0_24px_4px_rgb(34_211_238/10%)]" />
                {isLive && <span className="absolute left-4 top-4 rounded-md border border-rose-200 bg-rose-50/90 px-2.5 py-1 font-mono text-[10px] font-semibold tracking-[0.14em] text-rose-700">● ĐANG PHÂN TÍCH</span>}
              </div>
            </CardContent>
          </Card>

          <div className="grid grid-cols-2 gap-4 content-start">
            <MetricCard label="Tốc độ trung bình" value={number(liveSpeed, 1)} unit="km/h" icon={Gauge} tone="cyan" />
            <MetricCard label="Mật độ giao thông" value={number(liveDensity, 1)} unit="%" icon={Activity} tone="amber" />
            <MetricCard label="Số phương tiện" value={number(liveVehicleCount)} unit="xe" icon={CarFront} tone="violet" />
            <MetricCard label="Điểm rủi ro" value={number(analysis?.risk_score, 1)} unit="/ 100" icon={TriangleAlert} tone="rose" />

            <Card className="col-span-2 border-cyan-100 bg-white shadow-sm">
              <CardHeader className="pb-2">
                <div className="flex items-center justify-between gap-3">
                  <CardTitle className="flex items-center gap-2 text-sm text-[#123252]"><ScanLine className="size-4 text-cyan-700" /> Tốc độ từng phương tiện</CardTitle>
                  <Badge variant="outline" className="border-cyan-200 text-cyan-700">{liveVehicleCount ?? vehicles.length} xe trong khung hình</Badge>
                </div>
              </CardHeader>
              <CardContent>
                {vehicles.length > 0 ? (
                  <div className="grid max-h-44 gap-2 overflow-y-auto pr-1 sm:grid-cols-2">
                    {vehicles.slice(0, 12).map((vehicle, index) => (
                      <div key={`${vehicle.track_id}-${index}`} className="flex items-center justify-between gap-3 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
                        <div className="min-w-0">
                          <p className="truncate text-xs font-medium text-[#123252]">{vehicle.vehicle_type} · Xe số {vehicle.track_id >= 0 ? vehicle.track_id : 'mới'}</p>
                          <p className="mt-0.5 text-[10px] text-slate-600">Độ rõ {Math.round(vehicle.confidence * 100)}%</p>
                        </div>
                        <span className="shrink-0 font-mono text-sm font-semibold text-cyan-700">{vehicle.speed_kmh == null ? '—' : vehicle.speed_kmh.toFixed(1)} <small className="text-[9px] font-normal text-slate-500">km/h</small></span>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="text-sm leading-6 text-slate-500">Tốc độ từng phương tiện sẽ xuất hiện sau khi hệ thống theo dõi đủ vài khung hình.</p>
                )}
              </CardContent>
            </Card>

            <Card className="col-span-2 border-slate-200 bg-white shadow-sm">
              <CardHeader>
                <div className="flex items-center justify-between gap-3">
                  <CardDescription className="text-xs font-medium uppercase tracking-[0.15em] text-cyan-700">Cảnh báo ùn tắc</CardDescription>
                  {analysis && <Badge variant="outline" className={`border ${level?.badge} ${level?.color}`}>{analysis.anomaly ? 'Bất thường' : 'Bình thường'}</Badge>}
                </div>
                <CardTitle className={`mt-2 text-xl ${level?.color || 'text-[#123252]'}`}>
                  {analysis?.prediction_label || 'Đang chờ kết quả phân tích'}
                </CardTitle>
              </CardHeader>
              <CardContent>
                <p className="text-sm leading-6 text-slate-600">
                  {analysis
                    ? analysis.alert_title || `${level?.label}. Hệ thống đã đánh giá tốc độ trung bình, mật độ giao thông và số phương tiện.`
                    : 'Cảnh báo sẽ xuất hiện sau khi hệ thống nhận được dữ liệu đầu tiên từ video.'}
                </p>
              </CardContent>
            </Card>
          </div>
        </section>

        <Card className="border-slate-200 bg-white shadow-sm">
          <CardHeader className="pb-1">
            <CardTitle className="text-sm uppercase tracking-[0.16em] text-slate-500">Quy trình phân tích</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid gap-3 md:grid-cols-4">
              {pipeline.map(({ label, caption, icon: Icon, ready }, index) => (
                <div key={label} className={`pipeline-stage relative flex items-center gap-3 rounded-xl border p-4 ${ready ? 'is-ready border-cyan-200 bg-cyan-50' : 'border-slate-200 bg-slate-50'}`}>
                  <span className={`grid size-10 shrink-0 place-items-center rounded-lg ${ready ? 'bg-white text-cyan-700' : 'bg-white text-slate-500'}`}><Icon className="size-4" /></span>
                  <div className="min-w-0"><p className="flex items-center gap-1.5 text-sm font-medium text-[#123252]">{label}{ready && <CheckCircle2 className="size-3.5 text-emerald-600" />}</p><p className="mt-0.5 truncate text-xs text-slate-500">{caption}</p></div>
                  {index < pipeline.length - 1 && <span className="absolute -right-2 top-1/2 z-10 hidden size-4 -translate-y-1/2 rotate-45 border-r border-t border-cyan-200 bg-white md:block" />}
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        <section className="grid gap-5 lg:grid-cols-[minmax(0,1.5fr)_minmax(320px,.7fr)]">
          <Card className="border-slate-200 bg-white shadow-sm">
            <CardHeader>
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <CardTitle className="text-[#123252]">Diễn biến giao thông</CardTitle>
                  <CardDescription className="text-slate-500">32 lần cập nhật gần nhất từ video</CardDescription>
                </div>
                <div className="flex flex-wrap gap-2">
                  {chartLegend.map((item) => (
                    <span key={item.label} className="inline-flex items-center gap-2 rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 text-xs text-slate-600">
                      <span
                        className={`h-0 w-5 border-t-2 ${item.dashed ? 'border-dashed' : ''}`}
                        style={{ borderColor: item.color }}
                      />
                      {item.label}
                      <small className="text-slate-600">{item.unit}</small>
                    </span>
                  ))}
                </div>
              </div>
            </CardHeader>
            <CardContent>
              {history.length > 1 ? (
                <ChartContainer config={chartConfig} className="h-[260px] w-full aspect-auto">
                  <LineChart data={history} margin={{ left: -18, right: 8, top: 8 }}>
                    <CartesianGrid vertical={false} strokeDasharray="3 3" />
                    <XAxis dataKey="time" tickLine={false} axisLine={false} minTickGap={28} />
                    <YAxis domain={[0, 100]} tickLine={false} axisLine={false} />
                    <ChartTooltip content={<ChartTooltipContent indicator="line" />} />
                    <Line dataKey="speed" type="monotone" stroke="var(--color-speed)" strokeWidth={2.2} dot={false} />
                    <Line dataKey="density" type="monotone" stroke="var(--color-density)" strokeWidth={2.2} dot={false} />
                    <Line dataKey="risk" type="monotone" stroke="var(--color-risk)" strokeWidth={1.6} strokeDasharray="5 4" dot={false} />
                  </LineChart>
                </ChartContainer>
              ) : (
                <div className="grid h-[260px] place-items-center rounded-xl border border-dashed border-slate-200 bg-slate-50 text-center text-sm text-slate-500">
                  Biểu đồ sẽ hình thành sau hai chu kỳ dữ liệu.
                </div>
              )}
            </CardContent>
          </Card>

          <Card className="border-slate-200 bg-white shadow-sm">
            <CardHeader>
              <CardTitle className="text-[#123252]">Tóm tắt nguồn</CardTitle>
              <CardDescription className="text-slate-500">Những thông tin cần theo dõi khi demo</CardDescription>
            </CardHeader>
            <CardContent className="space-y-1">
              {[
                ['Vị trí', `${status?.latitude?.toFixed(4) ?? '—'}, ${status?.longitude?.toFixed(4) ?? '—'}`],
                ['Thời gian sự kiện', time(event?.timestamp)],
                ['Độ mượt nguồn', status ? `${status.fps.toFixed(1)} hình/giây` : '—'],
                ['Sự kiện đã gửi', status?.total_events?.toLocaleString('vi-VN') ?? '—'],
              ].map(([label, value]) => (
                <div key={label} className="flex items-center justify-between gap-4 border-b border-slate-100 py-3 last:border-0">
                  <span className="text-sm text-slate-500">{label}</span>
                  <span className="max-w-[58%] text-right font-mono text-xs text-[#123252]">{value}</span>
                </div>
              ))}
            </CardContent>
          </Card>
        </section>
      </div>
    </main>
  );
}
