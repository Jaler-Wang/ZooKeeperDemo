import java.util.ArrayList;
import java.util.List;

public class ChildrenCache {
    private List<String> children;

    public ChildrenCache(List<String> children){
        this.children = children;
    }

    public List<String> getList(){
        return children;
    }

    public List<String> addedAndSet(List<String> newChildren){
        List<String> diff = null;
        if(children == null){
            diff = new ArrayList<>(newChildren);
        }
        else{
            diff = getDifferentChildren(children, newChildren);
        }
        this.children = newChildren;
        return diff;
    }

    public List<String> removeAndSet(List<String> newChildren) {
        List<String> diff = null;
        if(children != null){
            diff = getDifferentChildren(newChildren, diff);
        }
        this.children = newChildren;
        return diff;
    }

    private List<String> getDifferentChildren(List<String> newChildren, List<String> children) {
        List<String> diff = null;
        if (children == null) {
            return null;
        }
        if(newChildren == null) return children;
        for(String s : children){
            if(!newChildren.contains(s)){
                if(diff == null){
                    diff = new ArrayList<>();
                }
                diff.add(s);
            }
        }
        return diff;
    }
}
