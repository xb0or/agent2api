//! 下游流式连接心跳保活：长时间没有数据时补一帧 SSE 注释 / WebSocket Ping。
//!
//! SSE 用注释行 `: keepalive`，不是 `data:` 事件 —— 不会进入任何客户端的
//! 事件回调，只让链路上持续有字节流动；WebSocket 用协议层 Ping（浏览器自动
//! 回 Pong），两者都能避免 Cloudflare 一类的空闲连接超时。
//!
//! 间隔与开关在每次构造时读一次配置：改设置后对**下一条**流生效，不需要重启。

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::Stream;

const SSE_KEEPALIVE: &[u8] = b": keepalive\n\n";

/// 给任意字节流套一层「静默超时补心跳」。
pub struct KeepaliveStream<S> {
    inner: S,
    interval: Option<Duration>,
    sleep: Pin<Box<tokio::time::Sleep>>,
}

impl<S> KeepaliveStream<S> {
    pub fn new(inner: S) -> Self {
        Self::with_interval(inner, crate::server::config::stream_keepalive_interval())
    }

    fn with_interval(inner: S, interval: Option<Duration>) -> Self {
        Self {
            inner,
            interval,
            sleep: Box::pin(tokio::time::sleep(interval.unwrap_or(Duration::from_secs(3600)))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::time::Duration;

    #[tokio::test]
    async fn emits_keepalive_when_inner_is_pending() {
        let mut stream = KeepaliveStream::with_interval(
            futures::stream::pending::<Result<Bytes, std::io::Error>>(),
            Some(Duration::from_millis(10)),
        );
        let item = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("心跳应在超时前产出")
            .expect("流不应结束")
            .expect("心跳帧不应报错");
        assert_eq!(item, Bytes::from_static(SSE_KEEPALIVE));
    }

    #[tokio::test]
    async fn disabled_stream_never_emits() {
        let mut stream = KeepaliveStream::with_interval(
            futures::stream::pending::<Result<Bytes, std::io::Error>>(),
            None,
        );
        assert!(tokio::time::timeout(Duration::from_millis(60), stream.next()).await.is_err());
    }
}

impl<S> Stream for KeepaliveStream<S>
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Unpin,
{
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(item) => {
                if let Some(interval) = self.interval {
                    self.sleep
                        .as_mut()
                        .reset(tokio::time::Instant::now() + interval);
                }
                Poll::Ready(item)
            }
            Poll::Pending => {
                let Some(interval) = self.interval else {
                    return Poll::Pending;
                };
                if self.sleep.as_mut().poll(cx).is_ready() {
                    self.sleep
                        .as_mut()
                        .reset(tokio::time::Instant::now() + interval);
                    Poll::Ready(Some(Ok(Bytes::from_static(SSE_KEEPALIVE))))
                } else {
                    Poll::Pending
                }
            }
        }
    }
}
