package introdb.heap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CustomUnorderedHeapFileTest {

    private Path heapFilePath;
    private Store heapFile;

    @BeforeEach
    public void setUp() throws IOException {
        heapFilePath = Files.createTempFile("heap", "0001");
        heapFile = new UnorderedHeapFile(heapFilePath, 1024, 4 * 1024);
    }

    @AfterEach
    public void tearDown() throws IOException {
        Files.delete(heapFilePath);
    }

    @Test
    void put_and_get_one_record() throws IOException, ClassNotFoundException {
        // given
        var firstkey = "1";
        var firstvalue = "value1";
        var entry = new Entry(firstkey, firstvalue);

        // when
        heapFile.put(entry);

        // then
        assertEquals(firstvalue, heapFile.get(firstkey));
    }

    @Test
    void put_and_delete_record() throws IOException, ClassNotFoundException {

        // given
        var key = "1";
        var value = "value1";

        // when
        heapFile.put(new Entry(key, value));
        final Object removed = heapFile.remove(key);

        // then
        assertNull(heapFile.get(key));
        assertThat(removed).isEqualTo(value);
    }

}
