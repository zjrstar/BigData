package msgpackcodec;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class EchoClientHandler extends ChannelInboundHandlerAdapter {

  private final int sendNumber;

  public EchoClientHandler(int sendNumber) {
    this.sendNumber = sendNumber;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    UserInfo[] infos = UserInfo();
    for (UserInfo info : infos) {
      ctx.writeAndFlush(info);
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    System.out.println("Client receive the msgpack message :" + msg);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    cause.printStackTrace();
    ctx.close();
  }

  private UserInfo[] UserInfo() {
    UserInfo[] userInfos = new UserInfo[sendNumber];
    UserInfo userInfo = null;
    for (int i = 0; i < sendNumber; i++) {
      userInfo = new UserInfo();
      userInfo.setAge(i);
      userInfo.setName("ABCEFG --->" + i);
      userInfos[i] = userInfo;
    }
    return userInfos;
  }
}
