package pojo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 分页基础对象，带分页的结果统一构造此对象返回
 * Created at 2017/5/24
 */
@Getter
@Setter
@ToString
public class Page<T> implements Serializable {

    private long totalCount;
    private int currentPage;
    private int pageSize;
    private long totalPage;
    private List<T> items;

    public Page(List<T> items, long totalCount, int pageSize, int currentPage) {
        this.items = items;
        this.totalCount = totalCount;
        this.pageSize = pageSize;
        this.currentPage = currentPage;
        this.totalPage = totalCount / (long) pageSize;
        if (totalCount % (long) pageSize > 0L) {
            ++this.totalPage;
        }
    }

    public Page() {

    }
}
