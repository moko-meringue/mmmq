package org.mmmq.broker.dispatcher.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.mmmq.broker.persistence.PersistenceProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Dispatcher 설정이 재기동을 넘어 살아남게 하는 {@code dispatchers.json}의 입출력 담당.
 *
 * <p>{@link org.mmmq.broker.dispatcher.DispatcherContainer}가 부팅 시 한 번 읽고 뮤테이션마다 전체를 다시 쓴다.
 * 쓰기는 임시 파일에 {@link StandardOpenOption#SYNC}로 디스크까지 내린 뒤 원자적으로 교체한다. 이 파일에서
 * 읽히는 것은 언제나 옛 내용 아니면 완전한 새 내용이고, 반쯤 쓰인 JSON은 나오지 않는다 — 전원이 나가도 그렇다.
 * {@code SYNC} 없이 교체만 하면 이름은 바뀌었는데 내용이 아직 디스크에 없는 상태가 가능하고, 그러면 다음 부팅에서
 * 0바이트 파일을 파싱하다 터져 브로커가 아예 뜨지 못한다.
 *
 * <p>교체(rename) 자체는 fsync하지 않으므로 전원이 나가면 <b>마지막 변경 하나를 잃을 수 있다.</b> 그건 감수한다 —
 * 막으려면 부모 디렉터리를 fsync해야 하는데 Windows에서는 디렉터리를 열 수 없어 터지고, broker는 라이브러리다.
 * 설정 변경 하나는 다시 넣으면 되지만 브로커가 뜨지 못하는 것은 다른 급이라, 비싼 쪽만 막았다.
 *
 * <p>{@code ObjectMapper}를 스프링 컨테이너로부터 주입받지 않고 직접 만드는 이유는 broker가 라이브러리라서다 —
 * 호스트 애플리케이션의 커스터마이징에 디스크 포맷이 휘둘리면 안 된다.
 */
@Component
public class DispatchersFile {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String TEMP_SUFFIX = ".tmp";
    private static final String EMPTY_ARRAY = "[]";

    private static final Logger log = LoggerFactory.getLogger(DispatchersFile.class);

    private final Path path;

    public DispatchersFile(PersistenceProperties properties) {
        path = properties.dispatchersFilePath();
        if (Files.exists(path)) {
            return;
        }
        try {
            Files.createDirectories(path.getParent());
            Files.writeString(
                    path,
                    EMPTY_ARRAY,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.SYNC
            );
            log.info("Dispatcher file not found. Created empty file at {}.", path);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to create dispatcher file: " + path, exception);
        }
    }

    public List<DispatcherEntry> read() {
        try {
            return List.of(OBJECT_MAPPER.readValue(Files.readAllBytes(path), DispatcherEntry[].class));
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to read dispatcher file: " + path, exception);
        }
    }

    public void write(List<DispatcherEntry> records) {
        Path temp = path.resolveSibling(path.getFileName() + TEMP_SUFFIX);
        try {
            Files.write(
                    temp,
                    OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(records),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.SYNC
            );
            Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to write dispatcher file: " + path, exception);
        }
    }
}
