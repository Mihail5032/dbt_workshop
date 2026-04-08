package ru.x5.factory;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import ru.x5.config.PropertiesHolder;

/**
 * Factory to create StreamExecutionEnvironment according property (local or cluster mode)
 */
public class StreamExecutionEnvironmentFactory {

    public static StreamExecutionEnvironment getStreamExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(180_000); // интервал 3 минуты
        env.getCheckpointConfig().setCheckpointTimeout(1_200_000L); // таймаут 10 минут (было default 10 мин, но barrier не проходит)
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(120_000L); // пауза 2 мин между чекпоинтами
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10); // не падать после N фейлов подряд
        env.getCheckpointConfig().enableUnalignedCheckpoints(); // barrier не ждёт выравнивания — проходит сквозь буферы
        return env;
    }
}