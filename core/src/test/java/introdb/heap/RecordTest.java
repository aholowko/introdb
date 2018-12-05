package introdb.heap;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

class RecordTest {

    private static final int PAGE_SIZE = 4 * 1024;

    @Test
    void shouldWriteAndReadEntry() throws IOException, ClassNotFoundException {
        //given
        final Entry entry = new Entry("a", "AA");
        final Record record = Record.of(entry);
        final ByteBuffer page = ByteBuffer.allocate(PAGE_SIZE);

        //when
        record.writeTo(page);
        page.rewind();
        final Record readRecord = Record.readFrom(page);
        final Entry readEntry = readRecord.entry();


        final byte b = page.get();

        //then
        assertThat(readEntry).isEqualTo(entry);
    }

    @Test
    void shouldReturnNullWhenRecordNotFoundInEmptyPage() {
        //given
        final ByteBuffer page = ByteBuffer.allocate(0);

        //when
        final Record readRecord = Record.readFrom(page);

        //then
        assertThat(readRecord).isNull();
    }
}
