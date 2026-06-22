---
name: readme-slim-design
description: README.md를 공식 문서 링크만 남기는 극도 간결형으로 개편하는 설계
metadata:
  type: project
---

# README 간결화 설계

## 목표

README.md에서 프로젝트 관련 구체적인 내용을 모두 제거하고,
공식 문서 사이트(mmmq.org) 링크와 참여자 정보만 남긴다.

## 변경 범위

### 1. `docs/docs/index.html` 신규 추가

`mmmq.org/docs/`가 항상 최신 버전으로 이동하도록 메타 리프레시 리다이렉트 파일을 추가한다.
새 버전 출시 시 이 파일의 리다이렉트 경로만 수정하면 된다.

- 경로: `docs/docs/index.html`
- 리다이렉트 대상: `./0.0.2/`

### 2. `README.md` 전면 개편

#### 제거 항목

- 전체 메시지 흐름 다이어그램 (아스키 코드 블록)
- 부팅 시 복원 흐름
- 모듈별 상세 설명 (클래스 표, 코드 예시, 설정 YAML, 디렉토리 레이아웃, 예외 계층 등)
- 아키텍처 이미지

#### 유지 항목

- 프로젝트 소개 1단락
- 문서 링크 테이블
- 참여자 섹션 (기존 GitHub 프로필 테이블 그대로)
- 라이선스

#### 최종 구조

```
# MMMQ (Moko-Meringue's Message Queue)

[1단락 소개]

## 문서

| 항목 | 링크 |
|------|------|
| 빠른 시작 | mmmq.org/quickstart.html |
| 레퍼런스 문서 | mmmq.org/docs/ |
| 릴리스 | mmmq.org/release.html |

## 참여자

[기존 프로필 테이블]

## 라이선스

[기존 문구]
```

## 비고

- 언어: 한국어 유지
- `mmmq.org/docs/`는 신규 추가하는 리다이렉트 파일로 최신 버전 보장
