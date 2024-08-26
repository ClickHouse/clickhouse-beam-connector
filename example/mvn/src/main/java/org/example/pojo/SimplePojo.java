package org.example.pojo;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

@DefaultSchema(JavaFieldSchema.class)
public class SimplePojo {
    public byte byteField;
    public short shortField;
    public int intField;
    public long longField;
    public float floatField;
    public double doubleField;
    public boolean booleanField;
    public String stringField;

    public SimplePojo() {
        this.byteField = RandomUtils.nextBytes(1)[0];
        this.shortField = (short)RandomUtils.nextInt(0, Short.MAX_VALUE);
        this.intField = RandomUtils.nextInt();
        this.longField = RandomUtils.nextLong();
        this.floatField = RandomUtils.nextFloat();
        this.doubleField = RandomUtils.nextDouble();
        this.booleanField = RandomUtils.nextBoolean();
        this.stringField = RandomStringUtils.randomAlphabetic(10);
    }

}
