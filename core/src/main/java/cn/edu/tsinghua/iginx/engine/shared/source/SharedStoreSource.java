package cn.edu.tsinghua.iginx.engine.shared.source;

public class SharedStoreSource extends AbstractSource {

    private final String key;

    public SharedStoreSource(String key) {
        super(SourceType.SharedStore);
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("fragment shouldn't be null");
        }
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public Source copy() {
        return new SharedStoreSource(key);
    }
}
