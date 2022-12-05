package simpledb.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class LruCache<K, V>  {
    private Map<K,DLinkedNode> pageCache;
    public ArrayList<K> getPageIdList() {
        ArrayList<K> list = new ArrayList<>();
        for (K k: pageCache.keySet()){
            list.add(k);
        }
        return list;
    }

    public ArrayList<V> getPageList() {
        ArrayList<V> list = new ArrayList<>();
        DLinkedNode dLinkedNode = tail.prev;
        while (dLinkedNode != head){
            list.add(dLinkedNode.value);
            dLinkedNode = dLinkedNode.prev;
        }
        return list;
    }


    private int size = 0;
    public int getSize() {
        return size;
    }


    private int capacity = BufferPool.DEFAULT_PAGES;
    private DLinkedNode head,tail;
    

    class  DLinkedNode{
        K key;
        V value;
        DLinkedNode next;
        DLinkedNode prev;
       
        public  DLinkedNode(){};
        public  DLinkedNode(K key,V value){
            this.key = key;
            this.value = value;
        }

    }

    public LruCache(int capacity) {
        this.pageCache = new HashMap<>();
        this.capacity = capacity;
        this.size = 0;

        head = new DLinkedNode();
        tail = new DLinkedNode();
        head.next = tail;
        tail.prev = head;

    }

    public V get(K key) {
        DLinkedNode node = pageCache.get(key);
        if (node == null) {
            return null;
        }
       
        moveToHead(node);
        return node.value;
    }
    
    public void put(K key, V value) {
        DLinkedNode node = pageCache.get(key);
        if (node == null) {     
            DLinkedNode newNode = new DLinkedNode(key, value);

            pageCache.put(key,newNode);
            addToHead(newNode);
            this.size++;
                   
        }
        else {
            node.value = value;
            moveToHead(node);
        }

        
    }

     private void addToHead(DLinkedNode node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    private void removeNode(DLinkedNode node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    private void moveToHead(DLinkedNode node) {
        removeNode(node);
        addToHead(node);
    }

    private DLinkedNode removeTail() {
        DLinkedNode res = tail.prev;
        removeNode(res);
        return res;
    }


    public V getLastEvictPage(){
        if(this.size == 0) return null;
        DLinkedNode del = tail.prev;
        return del.value;
    }

    public V getPage(K pid){
        if(this.size == 0) return null;
        DLinkedNode ans =  pageCache.get(pid);
        return ans.value;
    }

    private V evictPage(){
        if(this.size == 0) return null;
        DLinkedNode del =  removeTail();
        pageCache.remove(del.key);
        this.size--;
        return del.value;
    }


    public V evictPage(K pid){
        if(this.size == 0) return null;
        DLinkedNode del =  pageCache.get(pid);
        if (del == null) return null;
        removeNode(del);
        pageCache.remove(del.key);
        this.size--;
        return del.value;
    }

    
}

