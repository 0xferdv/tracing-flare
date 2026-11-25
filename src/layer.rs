//! `tracing_subscriber::Layer`：把 tracing 事件路由到 Flare runtime.
//!
//! 设计原理：
//! - `enabled` 先做一次廉价的 target/级别预过滤；`on_event` 再做字段收集、CRITICAL 还原、
//!   格式化与非阻塞入队；
//! - 路由结果按线程缓存，`enabled` 与 `on_event` 共享，动态 service 变化时用 generation 失效；
//! - visitor/消息缓冲/渲染缓冲都复用，避免每事件多次分配；
//! - 整个 `on_event` 包在 `catch_unwind` 中，格式化/路由 panic 只计 `layer_panics`，
//!   不传播给业务线程；
//! - writer 线程内回写、layer 自身递归都被识别并计 `reentrant`，防止自锁.

use std::backtrace::Backtrace;
use std::cell::{Cell, RefCell};
use std::fmt::Write as _;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::engine::record::{Field, FieldValue, Record};
use smallvec::SmallVec;
use tracing::field::{Field as TracingField, Visit};
use tracing::subscriber::Interest;
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

use crate::format::{RenderInput, TextFormat};
use crate::level::FlareLevel;
use crate::logger::{CRITICAL_FIELD, LOGGER_FIELD, LOGGER_TARGET};
use crate::router::Router;

/// 单事件最多收集的结构化字段数.
///
/// 设计原因：防止异常事件携带无限字段撑爆记录；超出部分直接丢弃，不让单条日志无界增长.
const MAX_FIELDS: usize = 64;
/// `%(taskName)s` 最多收集的 span 层数.
///
/// 实际渲染只用第一个 span，但保留上限避免未来扩展时无界遍历.
const MAX_SPANS: usize = 32;
/// 每线程最多保留的 scratch 内存.
///
/// 设计原因：一条 16 MiB 消息不能让线程后续生命周期一直占着同等 TLS 内存；超过上限就 shrink.
const MAX_RETAINED_SCRATCH: usize = 256 * 1024;

thread_local! {
    /// 当前线程是否已经在 `on_event` 内，用于阻止 layer 自身递归.
    static IN_LAYER: Cell<bool> = const { Cell::new(false) };
    /// 复用的渲染缓冲；格式化结果只复制一次成 `Arc<str>`，避免每事件额外分配临时 String.
    static RENDER_BUF: RefCell<String> = const { RefCell::new(String::new()) };
    /// 复用的字段 visitor；消息缓冲与字段 Vec 保留容量，减少每事件两次分配.
    static VISITOR: RefCell<EventVisitor> = RefCell::new(EventVisitor::default());
    /// 最近一次 target → service 解析；`enabled` 与 `on_event` 共享，generation 变化时失效.
    static ROUTE_CACHE: RefCell<RouteCache> = const {
        RefCell::new(RouteCache {
            generation: 0,
            target: String::new(),
            entry: None,
        })
    };
    /// `record_debug` 字段值的复用缓冲，避免一次临时 String 分配.
    static FIELD_BUF: RefCell<String> = const { RefCell::new(String::new()) };
    /// tracing 字段名来自静态 callsite 元数据，缓存其 `Arc<str>` 避免逐事件分配 key.
    static FIELD_KEYS: RefCell<Vec<Arc<str>>> = const { RefCell::new(Vec::new()) };
}

/// 线程内单条路由缓存.
///
/// 字段：`generation` 是缓存对应的路由表版本；`target` 是 target 字符串；`entry` 是解析结果（可能为
/// None，表示无路由）.
struct RouteCache {
    generation: u64,
    target: String,
    entry: Option<Arc<crate::router::ServiceEntry>>,
}

/// `IN_LAYER` 的 RAII 复位器.
///
/// 参数/行为：构造前已把标志置 true；Drop 时无条件复位，保证 panic/unwind 后不会永久屏蔽后续事件.
struct LayerGuard;

impl Drop for LayerGuard {
    /// 复位 layer 递归标志.
    ///
    /// 参数：`self`；返回值：无.
    /// 即使 `forward` panic，`catch_unwind` 展开时也会执行本 Drop.
    fn drop(&mut self) {
        IN_LAYER.with(|flag| flag.set(false));
    }
}

/// Flare 的 tracing layer；clone 共享 router 与 formatter.
///
/// 字段：`router` 负责 target/service 路由与动态 service；`format` 是编译后的文本模板；`json_lines`
/// 决定输出 JSON Lines 还是文本.
#[derive(Clone)]
#[non_exhaustive]
pub struct FlareLayer {
    router: Arc<Router>,
    format: Arc<TextFormat>,
    #[cfg_attr(not(feature = "json"), allow(dead_code))]
    json_lines: bool,
}

impl FlareLayer {
    /// 创建 layer.
    ///
    /// 参数：`router` 是共享路由/服务表；`format` 是编译后的文本格式；
    /// `json_lines` 表示是否输出 JSON Lines 格式.
    /// 返回值：可 clone 的 layer.
    pub(crate) fn new(router: Arc<Router>, format: TextFormat, json_lines: bool) -> Self {
        Self {
            router,
            format: Arc::new(format),
            json_lines,
        }
    }

    /// 带线程缓存的 target 路由.
    ///
    /// 参数：`target` 是 tracing target.
    /// 返回值：匹配的 service 或无路由.
    /// 缓存键包含 router generation；动态 service 创建后 generation 增加，旧缓存自动失效.
    #[inline]
    fn route_cached(&self, target: &str) -> Option<Arc<crate::router::ServiceEntry>> {
        let generation = self.router.generation();
        ROUTE_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            if cache.generation == generation && cache.target == target {
                return cache.entry.clone();
            }
            let entry = self.router.route(target);
            cache.generation = generation;
            cache.target.clear();
            cache.target.push_str(target);
            cache.entry = entry.clone();
            entry
        })
    }

    /// 按模板选择文本快路径.
    ///
    /// 参数：`input` 是渲染输入；`caller` 是调用点；`timestamp` 是事件时间；`out` 是复用缓冲.
    /// 返回值：无.
    /// 默认模板走 header 缓存，`%(message)s` 直接复制，其他模板走通用 piece 循环.
    #[inline]
    fn render_text_into(
        &self,
        input: RenderInput<'_>,
        caller: Option<(&str, u32)>,
        timestamp: std::time::SystemTime,
        out: &mut String,
    ) {
        if self.format.is_default_template() {
            self.format
                .render_default_into(input, caller, timestamp, out);
        } else if self.format.is_message_only() {
            self.format.render_message_into(input.message, out);
        } else {
            self.format
                .render_with_location_into_at(input, caller, timestamp, out);
        }
    }

    /// 处理一条 tracing 事件：路由、级别过滤、字段收集、格式化、入队.
    ///
    /// 参数：`event` 是 tracing 事件；`ctx` 提供 span 作用域查询.
    /// 返回值：无.
    /// 该函数运行在 `on_event` 的 panic 隔离与重入保护内，使用线程局部 visitor/渲染缓冲；它不做文件
    /// I/O，也不阻塞.
    fn forward<S>(&self, event: &Event<'_>, ctx: Context<'_, S>)
    where
        S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    {
        VISITOR.with(|slot| {
            let mut visitor = slot.borrow_mut();
            visitor.reset();

            let stats = self.router.stats();
            stats.seen.add(1);
            let metadata = event.metadata();
            let reserved = metadata.target() == LOGGER_TARGET;

            // `Layer::enabled` 无法检查事件字段，因此动态 `Logger` 门面事件携带保留 target，
            // 在这里按 logger 名解析.名字通常由 `Logger::emit` 通过 TLS 提供；该字段保留给
            // 外部直接发往保留 target 的调用方作为回退.
            #[cfg(feature = "python-hierarchy")]
            let hierarchy_plan = if reserved {
                crate::logger::current_plan()
            } else {
                None
            };
            #[cfg(feature = "python-hierarchy")]
            let use_handlers = hierarchy_plan
                .as_ref()
                .is_some_and(|plan| plan.has_handlers());
            #[cfg(not(feature = "python-hierarchy"))]
            let use_handlers = false;

            let service = if reserved {
                if use_handlers {
                    // handler 模式不需要先解析隐式 service；仍需采集字段以判断 CRITICAL.
                    // logger 名来自 TLS，外部 emitter 走原有 field 回退.
                    crate::logger::with_current_logger(|name| {
                        visitor.skip_logger_field = name.is_some();
                    });
                    event.record(&mut *visitor);
                    None
                } else {
                    let current_route = crate::logger::with_current_logger(|name| {
                        visitor.skip_logger_field = name.is_some();
                        name.map(|name| self.router.route_name(name))
                    });
                    event.record(&mut *visitor);
                    match current_route {
                        Some(route) => route,
                        None => {
                            let Some(name) = visitor.logger_name() else {
                                stats.no_route.fetch_add(1, Ordering::Relaxed);
                                return;
                            };
                            self.router.route_name(name)
                        }
                    }
                }
            } else {
                let Some(service) = self.route_cached(metadata.target()) else {
                    stats.no_route.fetch_add(1, Ordering::Relaxed);
                    return;
                };
                if !service.enabled_for(*metadata.level()) {
                    stats.filtered.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                event.record(&mut *visitor);
                Some(service)
            };
            if service.is_none() && !use_handlers {
                stats.no_route.fetch_add(1, Ordering::Relaxed);
                return;
            }

            let critical = visitor.fields.iter().any(|(key, value)| {
                key.as_ref() == CRITICAL_FIELD
                    && matches!(value, FieldValue::Str(text) if text.as_ref() == "CRITICAL")
            });
            let original = FlareLevel::from_tracing(*metadata.level());
            let effective = if critical {
                FlareLevel::Critical
            } else {
                original
            };

            // 单 service 路径不分配；handler 路径通常只有 1–2 个落点，SmallVec 内联.
            let mut outputs: SmallVec<[Arc<crate::router::ServiceEntry>; 4]> = SmallVec::new();
            if let Some(service) = service {
                if reserved && !service.enabled_for(*metadata.level()) {
                    stats.filtered.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                if !service.accepts_event(original, critical) {
                    stats.filtered.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                outputs.push(service);
            } else {
                #[cfg(feature = "python-hierarchy")]
                {
                    let Some(plan) = hierarchy_plan.as_ref() else {
                        stats.no_route.fetch_add(1, Ordering::Relaxed);
                        return;
                    };
                    if plan
                        .effective_level
                        .is_some_and(|minimum| effective < minimum)
                    {
                        stats.filtered.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                    for service in plan.services_for(effective) {
                        // handler 只降低“哪些 service 参与”，service 自身阈值仍然生效.
                        if !service.enabled_for(*metadata.level())
                            || !service.accepts_event(original, critical)
                        {
                            continue;
                        }
                        outputs.push(service);
                    }
                    if outputs.is_empty() {
                        stats.filtered.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                }
                #[cfg(not(feature = "python-hierarchy"))]
                {
                    stats.no_route.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            }
            let primary = &outputs[0];

            // span 名只被 `%(taskName)s`（第一个 span）消费.默认格式不引用它；为每个事件遍历
            // scope 并给每个 span 分配 `String` 是纯开销.
            let spans = if self.format.needs_task_name() {
                collect_spans(event, &ctx)
            } else {
                Vec::new()
            };

            let caller = if reserved {
                crate::logger::current_caller()
            } else {
                None
            };
            // 时钟只采样一次：渲染出的 header 与记录元数据（供自定义 sink 使用）必须描述同一事件
            // 时刻.
            let created_at = std::time::SystemTime::now();
            let backtrace = if outputs.iter().any(|service| service.backtrace_on_error)
                && effective.levelno() >= 40
            {
                Some(Backtrace::force_capture().to_string())
            } else {
                None
            };
            // 单落点与多落点共用同一份渲染逻辑；多 handler 时按各落点 service 名/target 重新渲染，
            // 避免 B 文件头部写着 A 的 service 名.
            let render = |service_name: &str, target: &str| -> String {
                RENDER_BUF.with(|buffer| {
                    let mut buffer = buffer.borrow_mut();
                    #[cfg(feature = "json")]
                    if self.json_lines {
                        crate::format::render_json_into(
                            crate::format::JsonInput {
                                service: service_name,
                                target,
                                level: effective,
                                message: visitor.message.as_str(),
                                metadata,
                                fields: &visitor.fields,
                                timestamp: created_at,
                                caller,
                                backtrace: backtrace.as_deref(),
                            },
                            &mut buffer,
                        );
                    } else {
                        self.render_text_into(
                            RenderInput {
                                service: service_name,
                                target,
                                level: effective,
                                message: visitor.message.as_str(),
                                metadata,
                                spans: &spans,
                            },
                            caller,
                            created_at,
                            &mut buffer,
                        );
                        if let Some(backtrace) = backtrace.as_deref() {
                            // 堆栈是受信任的多行负载.在注入转义之后追加，保证栈帧可读.
                            buffer.push('\n');
                            buffer.push_str(backtrace);
                        }
                    }
                    #[cfg(not(feature = "json"))]
                    {
                        self.render_text_into(
                            RenderInput {
                                service: service_name,
                                target,
                                level: effective,
                                message: visitor.message.as_str(),
                                metadata,
                                spans: &spans,
                            },
                            caller,
                            created_at,
                            &mut buffer,
                        );
                        if let Some(backtrace) = backtrace.as_deref() {
                            buffer.push('\n');
                            buffer.push_str(backtrace);
                        }
                    }
                    // 把渲染分配直接移入 Record；不再做第二次 `Arc<str>` 复制.
                    std::mem::take(&mut *buffer)
                })
            };

            if outputs.len() == 1 {
                let rendered_target = if reserved {
                    primary.target.as_str()
                } else {
                    metadata.target()
                };
                let text = render(&primary.name, rendered_target);
                let mut record = Record::at(effective.core(), text, created_at)
                    .with_target(cached_target(rendered_target));
                for (key, value) in visitor.fields.drain(..) {
                    // `flare_logger` 是内部路由状态，不是用户字段.保留它会为每个门面事件分配
                    // key `String` + `Arc<str>` 和值 `Arc<str>`，并与 `metadata.target` 中已有的
                    // service 名重复.
                    if reserved && key.as_ref() == LOGGER_FIELD {
                        continue;
                    }
                    record = record.with_field(Field::new(key, value));
                }
                if critical || original == FlareLevel::Trace {
                    // 为结构化 sink 保留原始级别；文本已经由 formatter 渲染完成.
                    let label = effective.label();
                    if !record
                        .fields
                        .iter()
                        .any(|field| field.key.as_ref() == "flare_level")
                    {
                        record = record.with_field(Field::new("flare_level", label));
                    }
                }
                let record = if self.json_lines {
                    record.mark_json()
                } else {
                    record
                };
                let record = record.mark_sanitized();
                if primary.logger.try_log_record(record) {
                    stats.forwarded.add(1);
                } else {
                    stats.rejected.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                // 多 handler：逐落点渲染（service 名/target 正确），每个落点直接构造自己的 Record，
                // 最后消费者不需要 clone.
                let mut delivered = false;
                for service in outputs.iter() {
                    let text = render(&service.name, service.target.as_str());
                    let mut record = Record::at(effective.core(), text, created_at)
                        .with_target(cached_target(service.target.as_str()));
                    for (key, value) in &visitor.fields {
                        if reserved && key.as_ref() == LOGGER_FIELD {
                            continue;
                        }
                        record = record.with_field(Field::new(Arc::clone(key), value.clone()));
                    }
                    if critical || original == FlareLevel::Trace {
                        let label = effective.label();
                        if !record
                            .fields
                            .iter()
                            .any(|field| field.key.as_ref() == "flare_level")
                        {
                            record = record.with_field(Field::new("flare_level", label));
                        }
                    }
                    let record = if self.json_lines {
                        record.mark_json()
                    } else {
                        record
                    };
                    delivered |= service.logger.try_log_record(record.mark_sanitized());
                }
                visitor.fields.clear();
                if delivered {
                    stats.forwarded.add(1);
                } else {
                    stats.rejected.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    }
}

/// 字段名 intern 缓存.
///
/// 参数：`name` 是 tracing 静态字段名.
/// 返回值：共享 `Arc<str>`.
/// 缓存最多 64 项，满了整体清空；字段名来自 callsite 元数据，重复率极高.
fn cached_field_key(name: &str) -> Arc<str> {
    const MAX_CACHED_KEYS: usize = 64;
    FIELD_KEYS.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(key) = cache.iter().find(|key| key.as_ref() == name) {
            return Arc::clone(key);
        }
        let key: Arc<str> = Arc::from(name);
        if cache.len() >= MAX_CACHED_KEYS {
            cache.clear();
        }
        cache.push(Arc::clone(&key));
        key
    })
}

/// target 的线程内 `Arc` 缓存.
///
/// 参数：`target` 是路由 target.
/// 返回值：共享 `Arc<str>`.
/// 每条记录都携带 target，但值按 service/callsite 稳定；缓存最多 32 项，命中只增加引用计数.
fn cached_target(target: &str) -> Arc<str> {
    const MAX_CACHED_TARGETS: usize = 32;
    thread_local! {
        static CACHE: RefCell<Vec<Arc<str>>> = const { RefCell::new(Vec::new()) };
    }
    CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(value) = cache.iter().find(|cached| cached.as_ref() == target) {
            return Arc::clone(value);
        }
        let value: Arc<str> = Arc::from(target);
        if cache.len() >= MAX_CACHED_TARGETS {
            cache.clear();
        }
        cache.push(Arc::clone(&value));
        value
    })
}

/// 收集当前 span 作用域的名称.
///
/// 参数：`event` 是 tracing 事件；`ctx` 是 subscriber 上下文.
/// 返回值：从根到当前的 span 名列表（最多 `MAX_SPANS`）.
/// 只有模板引用 `%(taskName)s` 时才调用，避免默认格式为 span 付出分配成本.
fn collect_spans<S>(event: &Event<'_>, ctx: &Context<'_, S>) -> Vec<String>
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    if !event.is_contextual() {
        return Vec::new();
    }
    let Some(scope) = ctx.event_scope(event) else {
        return Vec::new();
    };
    let mut names = Vec::new();
    for span in scope.from_root().take(MAX_SPANS) {
        names.push(span.name().to_string());
    }
    names
}

impl FlareLayer {
    /// 记录一次 layer 提前拒绝的重入事件.
    ///
    /// 参数：`metadata` 是事件元数据，用于找到目标 service.
    /// 返回值：无.
    /// 重入事件在字段采集前就被拒绝，但仍要把丢弃计入目标引擎的 `DropReason::Reentrant`；
    /// 否则 `stats.reentrant` 与 engine `dropped` 是两套口径，容量核算会低估.
    fn note_reentrant(&self, metadata: &Metadata<'_>) {
        self.router.stats.reentrant.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "python-hierarchy")]
        if metadata.target() == LOGGER_TARGET {
            if let Some(plan) = crate::logger::current_plan() {
                if plan.has_handlers() {
                    let level = FlareLevel::from_tracing(*metadata.level());
                    for service in plan.services_for(level) {
                        service.logger.count_reentrant();
                    }
                    return;
                }
            }
        }
        let service = if metadata.target() == LOGGER_TARGET {
            crate::logger::with_current_logger(|name| {
                name.and_then(|name| self.router.route_name(name))
            })
        } else {
            self.route_cached(metadata.target())
        };
        if let Some(service) = service {
            service.logger.count_reentrant();
        }
    }
}

impl<S> Layer<S> for FlareLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    /// 注册 callsite 兴趣.
    ///
    /// 参数：`_metadata` 是 callsite 元数据；返回值：始终 `Interest::sometimes()`.
    /// 设计原因：service 级别/路由可在运行期变化，不能在编译期全局禁用一个 callsite.
    fn register_callsite(&self, _metadata: &'static Metadata<'static>) -> Interest {
        Interest::sometimes()
    }

    /// 事件进入 `on_event` 前的廉价预过滤.
    ///
    /// 参数：`metadata` 提供 target/level；`_ctx` 本实现不需要.
    /// 返回值：是否可能被某个 service 接受.
    /// 普通事件按 target 找到 service 后检查级别；保留 target 的 Logger 门面事件因 logger 名是字段
    /// 、这里拿不到，只检查“是否存在任何 service”.
    fn enabled(&self, metadata: &Metadata<'_>, _ctx: Context<'_, S>) -> bool {
        if metadata.target() == LOGGER_TARGET {
            // logger 名是事件字段，这里拿不到.只要存在任何活动 service 就放行；
            // 真正的名字/级别路由由 `on_event` 执行.
            if !self.router.has_services() {
                self.router.stats.no_route.fetch_add(1, Ordering::Relaxed);
                return false;
            }
            return true;
        }
        let Some(service) = self.route_cached(metadata.target()) else {
            self.router.stats.no_route.fetch_add(1, Ordering::Relaxed);
            return false;
        };
        if service.enabled_for(*metadata.level()) {
            true
        } else {
            self.router.stats.filtered.fetch_add(1, Ordering::Relaxed);
            false
        }
    }

    /// 处理事件入口：重入保护 + panic 隔离.
    ///
    /// 参数：`event` 是 tracing 事件；`ctx` 提供 span 查询.
    /// 返回值：无.
    /// 如果当前线程是 writer 或已经在 layer 内，直接计 `reentrant` 并返回；之后设置 `IN_LAYER` 并
    /// `catch_unwind(forward)`，panic 只计 `layer_panics`.
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        if crate::engine::runtime::is_writer_thread() {
            self.note_reentrant(event.metadata());
            return;
        }
        if IN_LAYER.with(|flag| flag.replace(true)) {
            self.note_reentrant(event.metadata());
            return;
        }
        let _guard = LayerGuard;
        let result = panic::catch_unwind(AssertUnwindSafe(|| self.forward(event, ctx)));
        if result.is_err() {
            self.router
                .stats
                .layer_panics
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// 复用的 tracing 字段 visitor.
///
/// 字段：`message` 保存消息正文；`logger` 保存动态 logger 名；`skip_logger_field` 表示本次事件已通
/// 过 TLS 提供名称，无需再复制字段；`fields` 保存结构化字段（key 已 intern）.
#[derive(Default)]
struct EventVisitor {
    message: String,
    logger: Option<String>,
    skip_logger_field: bool,
    fields: Vec<(Arc<str>, FieldValue)>,
}

impl EventVisitor {
    /// 为下一条事件清空状态.
    ///
    /// 参数：无；返回值：无.
    /// 消息缓冲超过上限时 shrink，避免一条大消息长期占 TLS 内存.
    fn reset(&mut self) {
        self.message.clear();
        if self.message.capacity() > MAX_RETAINED_SCRATCH {
            self.message.shrink_to(MAX_RETAINED_SCRATCH);
        }
        self.logger = None;
        self.skip_logger_field = false;
        self.fields.clear();
    }

    /// 追加一个结构化字段.
    ///
    /// 参数：`field` 是 tracing 字段元数据；`value` 是已转换的值.
    /// 返回值：无.
    /// 字段数达到 `MAX_FIELDS` 后静默丢弃，避免单条记录无界增长.
    fn push(&mut self, field: &TracingField, value: FieldValue) {
        if self.fields.len() >= MAX_FIELDS {
            return;
        }
        self.fields.push((cached_field_key(field.name()), value));
    }

    /// 记录动态 logger 名.
    ///
    /// 参数：`value` 是 `flare_logger` 字段值.
    /// 若 TLS 已提供名称（`Logger::emit` 路径），跳过复制，避免每事件分配.
    fn set_logger(&mut self, value: &str) {
        if !self.skip_logger_field {
            self.logger = Some(value.to_string());
        }
    }

    /// 返回动态 logger/service 名.
    ///
    /// 参数：无；返回值：已记录的名称或 `None`.
    /// 该名称只用于路由，不进入用户字段.
    fn logger_name(&self) -> Option<&str> {
        self.logger.as_deref()
    }
}

impl Visit for EventVisitor {
    /// 记录字符串字段.
    ///
    /// 参数：`field` 是字段元数据；`value` 是字符串值.
    /// 返回值：无.`message` 直接写入复用缓冲；`flare_logger` 走名称路径；其他字段复制为 `Arc<str>`.
    fn record_str(&mut self, field: &TracingField, value: &str) {
        match field.name() {
            "message" => {
                self.message.clear();
                self.message.push_str(value);
            }
            LOGGER_FIELD => self.set_logger(value),
            _ => self.push(field, FieldValue::Str(Arc::from(value))),
        }
    }

    /// 记录 Debug 字段.
    ///
    /// 参数：`field` 是字段元数据；`value` 是 Debug 值.
    /// 返回值：无.
    /// 消息与字段值都写入复用缓冲后再转 `Arc<str>`，少一次临时 String 分配.
    fn record_debug(&mut self, field: &TracingField, value: &dyn std::fmt::Debug) {
        match field.name() {
            "message" => {
                self.message.clear();
                let _ = write!(self.message, "{value:?}");
            }
            LOGGER_FIELD => {
                if !self.skip_logger_field {
                    self.logger = Some(format!("{value:?}"));
                }
            }
            _ => {
                let text = FIELD_BUF.with(|buffer| {
                    let mut buffer = buffer.borrow_mut();
                    buffer.clear();
                    let _ = write!(buffer, "{value:?}");
                    let text = Arc::<str>::from(buffer.as_str());
                    if buffer.capacity() > MAX_RETAINED_SCRATCH {
                        buffer.shrink_to(MAX_RETAINED_SCRATCH);
                    }
                    text
                });
                self.push(field, FieldValue::Str(text));
            }
        }
    }

    /// 记录 i64 字段.
    ///
    /// 参数：`field` 是字段元数据；`value` 是整数.
    /// 返回值：无.`message` 写入复用缓冲，其他字段保存原始类型供 JSON 使用.
    fn record_i64(&mut self, field: &TracingField, value: i64) {
        if field.name() == "message" {
            self.message.clear();
            let _ = write!(self.message, "{value}");
        } else {
            self.push(field, FieldValue::I64(value));
        }
    }

    /// 记录 u64 字段.
    ///
    /// 参数：`field` 是字段元数据；`value` 是无符号整数.
    /// 返回值：无.
    /// 规则同 `record_i64`.
    fn record_u64(&mut self, field: &TracingField, value: u64) {
        if field.name() == "message" {
            self.message.clear();
            let _ = write!(self.message, "{value}");
        } else {
            self.push(field, FieldValue::U64(value));
        }
    }

    /// 记录 f64 字段.
    ///
    /// 参数：`field` 是字段元数据；`value` 是浮点数.
    /// 返回值：无.`message` 写入复用缓冲，其他字段保存原始 f64.
    fn record_f64(&mut self, field: &TracingField, value: f64) {
        if field.name() == "message" {
            self.message.clear();
            let _ = write!(self.message, "{value}");
        } else {
            self.push(field, FieldValue::F64(value));
        }
    }

    /// 记录 bool 字段.
    ///
    /// 参数：`field` 是字段元数据；`value` 是布尔值.
    /// 返回值：无.`message` 写入复用缓冲，其他字段保存 Bool.
    fn record_bool(&mut self, field: &TracingField, value: bool) {
        if field.name() == "message" {
            self.message.clear();
            let _ = write!(self.message, "{value}");
        } else {
            self.push(field, FieldValue::Bool(value));
        }
    }
}
