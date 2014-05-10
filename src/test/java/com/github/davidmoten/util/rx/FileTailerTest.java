package com.github.davidmoten.util.rx;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.github.davidmoten.rx.FileTailer;

import rx.Subscription;
import rx.functions.Action1;
import rx.schedulers.Schedulers;

public class FileTailerTest {

    @Test
    public void testFileTailingFromStartOfFile() throws InterruptedException, IOException {
        File log = new File("target/test.log");
        log.delete();
        log.createNewFile();
        append(log, "a0");
        FileTailer tailer = FileTailer.builder().file(log).startPositionBytes(0).build();
        final List<String> list = new ArrayList<String>();
        Subscription sub = tailer.tail(50).subscribeOn(Schedulers.io()).subscribe(new Action1<String>() {
            @Override
            public void call(String line) {
                System.out.println("received: '" + line + "'");
                list.add(line);
            }
        });

        Thread.sleep(500);
        append(log, "a1");
        append(log, "a2");
        Thread.sleep(500);
        assertEquals(Arrays.asList("a0", "a1", "a2"), list);
        sub.unsubscribe();
    }

    private static void append(File file, String line) {
        try {
            FileOutputStream fos = new FileOutputStream(file, true);
            fos.write(line.getBytes());
            fos.write('\n');
            fos.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
