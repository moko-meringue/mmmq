<!-- @formatter:off -->
# MMMQ (Moko-Meringue's Message Queue)

# 🎯 프로젝트 목적

**MMMQ**는 메시지 큐 시스템의 핵심 개념과 동작 원리를 학습하기 위한 프로젝트입니다.

- 단순히 도구를 사용하는 것을 넘어, 메시지 브로커의 존재 이유와 동작 메커니즘을 근본적으로 이해합니다.
- 큐 자료구조부터 메시지 영속화, 동시성 제어까지 바닥부터 직접 쌓아 올립니다.
- 오픈 소스 방식의 협업과 피드백을 통해 '함께 성장하는 소프트웨어'를 지향합니다.

# 🏗️ 아키텍처

## 시스템 흐름

![전체_아키텍처](docs/images/전체_아키텍처.png)

Producer는 메시지를 생성하여 Broker로 전송합니다.
Broker는 메시지를 큐에 저장하고, Consumer가 메시지를 요청할 때 이를 전달합니다.
만약 메시지 처리에 실패하면, 메시지는 Dead Letter Queue로 이동하여 별도로 관리됩니다.
Dead Letter Queue에 저장된 메시지는 디스크에 나중에 재처리하거나 분석할 수 있습니다.

## 주요 컴포넌트

### Producer

`Producer`는 메시지를 생성하고 브로커에게 전송합니다. 토픽과 함께 메시지를 발행할 수 있으며, 전송 실패 시 설정된 횟수만큼 재시도를 수행합니다.  
  
### Broker

`Broker`는 Producer로부터 메시지를 받아 Consumer에게 전달합니다. 메시지 수신을 위한 엔드포인트를 제공하며, `FrontDispatcher`를 통해 메시지를 적절한 `Dispatcher`에게 전달합니다.  
  
`FrontDispatcher`는 Broker로 들어온 메시지의 토픽을 분석하여 해당 토픽을 구독 중인 모든 `Dispatcher`에게 메시지를 분배합니다.  
  
`Dispatcher`는 메시지 큐를 관리하고 Consumer에게 메시지를 전달합니다. 각 Consumer마다 별도의 `Dispatcher`가 할당되며, 내부적으로 `BlockingQueue`를 사용하여 메시지를 관리합니다. 각 `Dispatcher`는 독립된 스레드 풀을 소유하여 특정 Consumer의 느린 메시지 소비가 다른 Consumer에게 영향을 미치는 **느린 소비자 문제**를 해결합니다.  
  
`DeadLetterQueue`는 메시지 전송에 최종적으로 실패했을 때 해당 메시지를 처리합니다. 전송 실패 원인과 함께 메시지를 `DeadLetter`로 포장하여 저장함으로써, 추후 분석 및 복구를 지원합니다. 재시도 횟수 초과, 처리 불가 예외 발생 등 다양한 실패 상황에 대응합니다.  
  
### Consumer

`FrontHandler`는 수신한 메시지의 토픽을 분석하여 해당 토픽을 구독 중인 모든 `HandlerExecution`들을 찾아 실행합니다.  
  
`HandlerExecution`은 어노테이션 기반(`@MMMQListener`)과 인터페이스 구현(`MMMQHandler`) 방식을 모두 동일한 방식으로 처리할 수 있도록 추상화된 메시지 핸들러 실행부입니다. 이를 통해 다양한 방식의 메시지 핸들러 실행부의 일관성을 유지하고 확장성을 높입니다.  
  
`@MMMQListener` 어노테이션을 사용하면 메서드에 `@MMMQListener(topic = "...")`를 붙여 메시지 핸들러를 지정할 수 있습니다. Spring의 `@EventListener`처럼, 선언적으로 특정 토픽의 메시지를 처리할 메서드를 간편하게 등록할 수 있습니다.  
  
`MMMQListener` 인터페이스를 구현하여 메시지 핸들러를 구현할 수도 있습니다. 컴파일 타임에 타입 안정성을 제공하며, 복잡한 메시지 처리 로직을 구현할 때 유용합니다.  
  
# 👥 참여자

모든 코드는 모코와 머랭의 **페어 프로그래밍**으로 작성되었습니다.

<div align="center">
<table>
  <tr height="140px">
    <td align="center">
      <a href="https://github.com/cookie-meringue">
        <img src="https://avatars.githubusercontent.com/u/113977176?v=4" alt="머랭" width="100" />
      </a>
      <br />
      <a href="https://github.com/cookie-meringue">머랭</a>
    </td>
      <td align="center">
      <a href="https://github.com/songsunkook">
        <img src="https://avatars.githubusercontent.com/u/21010656?v=4" alt="모코" width="100" />
      </a>
      <br />
      <a href="https://github.com/songsunkook">모코</a>
    </td>
  </tr>
</table>
</div>

# ⚖️ 라이선스

이 프로젝트는 교육 목적으로 제작되었습니다.
