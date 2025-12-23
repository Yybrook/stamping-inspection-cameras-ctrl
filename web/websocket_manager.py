from fastapi import WebSocket
import typing


class Manager:
    def __init__(self):
        # 当前存活的 WebSocket 连接对象列表
        self.alive: typing.List[WebSocket] = list()

    async def connect(self, ws: WebSocket):
        """新客户端连接时调用"""
        # 接受客户端的握手请求
        await ws.accept()
        # 把 WebSocket 加入存活列表
        self.alive.append(ws)

    def disconnect(self, ws: WebSocket):
        """断开客户端时调用"""
        # 把 WebSocket 从存活列表中删除
        try:
            self.alive.remove(ws)
        except ValueError:
            pass

    # broadcast 中异常移除失效连接
    async def broadcast(self, message: dict):
        """群发消息给所有存活的 WebSocket"""
        to_remove = list()

        for ws in self.alive:
            try:
                # 将 message 以json格式发送
                await ws.send_json(message)
            except:
                to_remove.append(ws)

        # 断开无效连接
        for ws in to_remove:
            self.disconnect(ws)

    @property
    def survival(self) -> bool:
        """是否还有存活连接"""
        return bool(self.alive)

    @property
    def quantity(self) -> int:
        """当前存活的连接数量"""
        return len(self.alive)


class HybridManager:
    def __init__(self):
        # hybrid: tag -> Manager。
        # 可以根据 tag（标签/频道）区分不同的连接组，每个组单独管理
        self.hybrid: typing.Dict[str, Manager] = dict()

    async def connect(self, tag: str, ws: WebSocket):
        """
        给某个 tag 添加 WebSocket, 并连接
        :param tag:
        :param ws:
        :return:
        """
        await self.hybrid.setdefault(tag, Manager()).connect(ws)

    def disconnect(self, tag: str, ws: WebSocket):
        """从某个 tag 对应的组里移除连接"""
        if tag in self.hybrid:
            self.hybrid[tag].disconnect(ws)

    # broadcast 中异常移除失效连接
    async def broadcast(self, tag: str, message: dict):
        """群发消息给某个 tag 的所有连接"""
        if tag in self.hybrid:
            await self.hybrid[tag].broadcast(message)

    def survival(self, tag: str) -> bool:
        """判断某个 tag 下是否还有存活连接"""
        return tag in self.hybrid and self.hybrid[tag].survival

    def quantity(self, tag: str) -> int:
        """返回某个 tag 下的连接数量"""
        return self.hybrid[tag].quantity if tag in self.hybrid else 0

# 创建一个全局的 WebSocket 管理器，可以在 FastAPI 的路由或后台任务中使用
ws_manager = HybridManager()
