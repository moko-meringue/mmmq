# 삭제된 Dispatcher가 되살아나던 경로

- 작성일: 2026-08-01
- 대상: `broker/dispatcher/Dispatcher.java`의 `WorkerPool`

## 왜 이 문서가 있나

`Dispatcher.subscribe`가 구독과 동시에 워커를 만들고(`WorkerPool.open`), `shutdownAll()`이 맵을 비우지 않는다. 코드만 보면 둘 다 없어도 될 것처럼 보인다 — 워커는
필요할 때 만들면 되고, 종료했으면 치우는 게 자연스럽다.

그런데 그 "자연스러운" 형태가 **삭제된 Dispatcher가 메시지를 다시 보내는 경로**를 연다. 이 문서는 그 흐름을 적어 둔 것이다.

## 두 설계

|          | 워커 생성 시점             | `shutdownAll()`                  |
|----------|----------------------|----------------------------------|
| 지금       | `subscribe` (구독과 동시) | `shutdownNow()`만                 |
| 문제 있는 형태 | `dispatch` (지연 생성)   | `shutdownNow()` + `pool.clear()` |

## 지금 설계에서의 흐름

**T1이 D1을 조회한다**

이 시점에 D1은 `order.new`·`order.shipped`를 구독 중이고, **보장되는 것은 그 둘 모두에 대해 스레드 풀을 보유 중**이라는 사실이다. `subscribe`가 구독과 워커 생성을 한 단위로
묶기 때문이다.

**T2가 D1을 제거한다**

`shutdownAll()`이 호출된다. `clear`는 하지 않으므로 맵에는 각 토픽 큐에 대한 스레드 풀이 남아 있다. 다만 **전부 셧다운된 상태**라 요청을 처리하지 못한다.

**T1이 D1의 `dispatch()`를 호출한다**

`submit(order.shipped)`가 종료된 워커를 찾고, `DiscardPolicy`가 작업을 조용히 버린다. **아무것도 전송되지 않는다.**

만약, `dispatch()`시점에 워커가 생성되는 "지연 생성" 구조였다면, D1은 `order.shipped`에 대한 스레드 풀이 없으므로 새로 만들고, **삭제된 Dispatcher가 메시지를 전송하게 되는
상황**이 발생한다.

그러므로, 아무것도 전송되지 않는 것이 정상이다.

## 원래 문제

**T1이 D1을 조회한다**

이 시점에 D1은 `order.new`·`order.shipped`를 구독 중이다. 그런데 워커는 지연 생성이라, **실제 `dispatch`가 실행될 때 해당 큐의 스레드 풀이 없으면 그제서야 만든다.** 지금
D1에는 `order.new`의 스레드 풀만 존재한다 — `order.shipped`로는 메시지가 온 적이 없기 때문이다.

**T2가 D1을 제거한다**

- `shutdownAll()` 호출 → D1이 보유한 모든 스레드 풀에 `shutdownNow()`
- 맵을 `clear()`
- 컨테이너가 구독 맵을 새 매칭 결과로 덮어쓰고, 빠진 짝의 체크포인트를 지운다

**여기서 `order.shipped`의 워커는 종료 대상이 아니었다.** 존재한 적이 없으니 `shutdownNow()`가 닿을 것도 없다.

**T1이 `order.shipped`로 D1의 `dispatch()`를 호출한다**

지연 생성이라 **이때 D1이 갑자기 `order.shipped`에 대한 스레드 풀을 만든다.** 인터럽트되지 않은 새 executor다. 그래서 **갑자기 D1이 동작한다** — 방금 삭제된 소비자의 host로
`order.shipped`를 소비해 보낸다.

## 왜 기존 가드가 못 막나

`dispatch`의 첫 줄이 `subscriptions.containsKey(topicQueue)`인데 이게 **통과한다.**

컨테이너는 Dispatcher를 삭제할 때 **구독을 해제하는 게 아니라 객체 참조를 버린다.** `rematchAll`이 덮어쓰는 것은 컨테이너의 `Map<TopicQueue, List<Dispatcher>>`
이고, 지워지는 것은 체크포인트 파일이다. **버려진 D1 인스턴스 자신의 `subscriptions` 맵은 그대로 남는다.**

즉 D1은 자기가 여전히 그 큐를 구독 중이라고 믿는다. 가드는 "이 큐를 구독한 적이 없다"를 보는 것이지 "내가 죽었다"를 보는 것이 아니다.

## 몇 건이나 나가나

타이밍에 달렸다. `drain`은 `peek → deliver → commit`을 돌고, `commit`이 삭제된 체크포인트를 만나면 `IllegalStateException`이 나서 바깥 `catch`가 루프를
끝낸다.

- 체크포인트 삭제가 이미 끝났으면 **한 건**(전송은 이미 나간 뒤 커밋에서 터진다)
- T1은 `mutationLock`을 잡지 않으므로 T2의 `rematchAll`과 **동시에 돌 수 있고**, 삭제가 아직 안 닿았으면 커밋이 성공해 **루프가 계속 돈다**
