package cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.chunk;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.Iterator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.PeriodDuration;
import org.apache.arrow.vector.complex.impl.DenseUnionWriter;
import org.apache.arrow.vector.complex.impl.UnionWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.*;
import org.apache.arrow.vector.holders.*;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;

public class DelegateFieldReader implements FieldReader {
  private final FieldReader delegate;

  public DelegateFieldReader(FieldReader delegate) {
    this.delegate = Preconditions.checkNotNull(delegate);
  }

  @Override
  public boolean next() {
    return delegate.next();
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public void copyAsValue(BaseWriter.MapWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsValue(BaseWriter.ListWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsValue(BaseWriter.StructWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public FieldReader reader(String name) {
    return delegate.reader(name);
  }

  @Override
  public Iterator<String> iterator() {
    return delegate.iterator();
  }

  @Override
  public FieldReader reader() {
    return delegate.reader();
  }

  @Override
  public void read(BigIntHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableBigIntHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(TimeMicroHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableTimeMicroHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(TimeNanoHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableTimeNanoHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(TimeStampMicroTZHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableTimeStampMicroTZHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(TimeStampMilliTZHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableTimeStampMilliTZHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(TimeStampNanoTZHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableTimeStampNanoTZHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(TimeStampSecTZHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableTimeStampSecTZHolder h) {
    delegate.read(h);
  }

  @Override
  public Long readLong() {
    return delegate.readLong();
  }

  @Override
  public void copyAsValue(UInt8Writer writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, UInt8Writer writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void copyAsValue(TimeStampSecTZWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, TimeStampSecTZWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void copyAsValue(TimeStampNanoTZWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, TimeStampNanoTZWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void copyAsValue(TimeStampMilliTZWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, TimeStampMilliTZWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void copyAsValue(TimeStampMicroTZWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, TimeStampMicroTZWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void copyAsValue(TimeNanoWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, TimeNanoWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void copyAsValue(TimeMicroWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, TimeMicroWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void copyAsValue(BigIntWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, BigIntWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void read(BitHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableBitHolder h) {
    delegate.read(h);
  }

  @Override
  public Boolean readBoolean() {
    return delegate.readBoolean();
  }

  @Override
  public void copyAsValue(BitWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, BitWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void read(DateDayHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableDateDayHolder h) {
    delegate.read(h);
  }

  @Override
  public void copyAsValue(DateDayWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, DateDayWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void read(DateMilliHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableDateMilliHolder h) {
    delegate.read(h);
  }

  @Override
  public void copyAsValue(DateMilliWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, DateMilliWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void read(Decimal256Holder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableDecimal256Holder h) {
    delegate.read(h);
  }

  @Override
  public void read(DecimalHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableDecimalHolder h) {
    delegate.read(h);
  }

  @Override
  public BigDecimal readBigDecimal() {
    return delegate.readBigDecimal();
  }

  @Override
  public void copyAsValue(DecimalWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, DecimalWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void copyAsValue(Decimal256Writer writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, Decimal256Writer writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void read(DurationHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableDurationHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(IntervalDayHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableIntervalDayHolder h) {
    delegate.read(h);
  }

  @Override
  public Duration readDuration() {
    return delegate.readDuration();
  }

  @Override
  public void copyAsValue(IntervalDayWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, IntervalDayWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void copyAsValue(DurationWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, DurationWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void read(FixedSizeBinaryHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableFixedSizeBinaryHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(LargeVarBinaryHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableLargeVarBinaryHolder h) {
    delegate.read(h);
  }

  @Override
  public byte[] readByteArray() {
    return delegate.readByteArray();
  }

  @Override
  public void copyAsValue(VarBinaryWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, VarBinaryWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void copyAsValue(LargeVarBinaryWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, LargeVarBinaryWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void copyAsValue(FixedSizeBinaryWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, FixedSizeBinaryWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void read(Float4Holder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableFloat4Holder h) {
    delegate.read(h);
  }

  @Override
  public Float readFloat() {
    return delegate.readFloat();
  }

  @Override
  public void copyAsValue(Float4Writer writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, Float4Writer writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void read(Float8Holder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableFloat8Holder h) {
    delegate.read(h);
  }

  @Override
  public Double readDouble() {
    return delegate.readDouble();
  }

  @Override
  public void copyAsValue(Float8Writer writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, Float8Writer writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void read(IntHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableIntHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(TimeSecHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableTimeSecHolder h) {
    delegate.read(h);
  }

  @Override
  public Integer readInteger() {
    return delegate.readInteger();
  }

  @Override
  public void copyAsValue(UInt4Writer writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, UInt4Writer writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void copyAsValue(TimeSecWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, TimeSecWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void copyAsValue(IntWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, IntWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void read(IntervalMonthDayNanoHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableIntervalMonthDayNanoHolder h) {
    delegate.read(h);
  }

  @Override
  public PeriodDuration readPeriodDuration() {
    return delegate.readPeriodDuration();
  }

  @Override
  public void copyAsValue(IntervalMonthDayNanoWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, IntervalMonthDayNanoWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void read(IntervalYearHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableIntervalYearHolder h) {
    delegate.read(h);
  }

  @Override
  public Period readPeriod() {
    return delegate.readPeriod();
  }

  @Override
  public void copyAsValue(IntervalYearWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, IntervalYearWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void read(LargeVarCharHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableLargeVarCharHolder h) {
    delegate.read(h);
  }

  @Override
  public void copyAsValue(LargeVarCharWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, LargeVarCharWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void read(SmallIntHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableSmallIntHolder h) {
    delegate.read(h);
  }

  @Override
  public Short readShort() {
    return delegate.readShort();
  }

  @Override
  public void copyAsValue(SmallIntWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, SmallIntWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void read(TimeMilliHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableTimeMilliHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(TimeStampMicroHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableTimeStampMicroHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(TimeStampMilliHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableTimeStampMilliHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(TimeStampNanoHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableTimeStampNanoHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(TimeStampSecHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableTimeStampSecHolder h) {
    delegate.read(h);
  }

  @Override
  public LocalDateTime readLocalDateTime() {
    return delegate.readLocalDateTime();
  }

  @Override
  public void copyAsValue(TimeStampSecWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, TimeStampSecWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void copyAsValue(TimeStampNanoWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, TimeStampNanoWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void copyAsValue(TimeStampMilliWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, TimeStampMilliWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void copyAsValue(TimeStampMicroWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, TimeStampMicroWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void copyAsValue(TimeMilliWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, TimeMilliWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void read(TinyIntHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableTinyIntHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(UInt1Holder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableUInt1Holder h) {
    delegate.read(h);
  }

  @Override
  public void read(UInt2Holder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableUInt2Holder h) {
    delegate.read(h);
  }

  @Override
  public void read(UInt4Holder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableUInt4Holder h) {
    delegate.read(h);
  }

  @Override
  public void read(UInt8Holder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableUInt8Holder h) {
    delegate.read(h);
  }

  @Override
  public void read(VarBinaryHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableVarBinaryHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(VarCharHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(NullableVarCharHolder h) {
    delegate.read(h);
  }

  @Override
  public void read(Float2Holder float2Holder) {
    delegate.read(float2Holder);
  }

  @Override
  public void read(NullableFloat2Holder nullableFloat2Holder) {
    delegate.read(nullableFloat2Holder);
  }

  @Override
  public Object readObject() {
    return delegate.readObject();
  }

  @Override
  public Text readText() {
    return delegate.readText();
  }

  @Override
  public void copyAsValue(VarCharWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, VarCharWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public Character readCharacter() {
    return delegate.readCharacter();
  }

  @Override
  public void copyAsValue(UInt2Writer writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, UInt2Writer writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public Byte readByte() {
    return delegate.readByte();
  }

  @Override
  public void copyAsValue(UInt1Writer writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, UInt1Writer writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public void copyAsValue(TinyIntWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void copyAsField(String name, TinyIntWriter writer) {
    delegate.copyAsField(name, writer);
  }

  @Override
  public Field getField() {
    return delegate.getField();
  }

  @Override
  public Types.MinorType getMinorType() {
    return delegate.getMinorType();
  }

  @Override
  public void reset() {
    delegate.reset();
  }

  @Override
  public void read(UnionHolder holder) {
    delegate.read(holder);
  }

  @Override
  public void read(int index, UnionHolder holder) {
    delegate.read(index, holder);
  }

  @Override
  public void copyAsValue(UnionWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public void read(DenseUnionHolder holder) {
    delegate.read(holder);
  }

  @Override
  public void read(int index, DenseUnionHolder holder) {
    delegate.read(index, holder);
  }

  @Override
  public void copyAsValue(DenseUnionWriter writer) {
    delegate.copyAsValue(writer);
  }

  @Override
  public boolean isSet() {
    return delegate.isSet();
  }

  @Override
  public void copyAsValue(Float2Writer float2Writer) {
    delegate.copyAsValue(float2Writer);
  }

  @Override
  public void copyAsField(String s, Float2Writer float2Writer) {
    delegate.copyAsField(s, float2Writer);
  }

  @Override
  public int getPosition() {
    return delegate.getPosition();
  }

  @Override
  public void setPosition(int index) {
    delegate.setPosition(index);
  }
}
