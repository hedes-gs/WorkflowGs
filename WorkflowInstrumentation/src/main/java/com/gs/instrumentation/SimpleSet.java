package com.gs.instrumentation;

public class SimpleSet<T> {
    protected Object[] objects = new Object[20];

    public void add(T o) {
        for (int k = 0; k < this.objects.length; k++) {
            if (this.objects[k] == null) {
                this.objects[k] = o;
                break;
            } else if (this.objects[k].equals(o)) {
                break;
            }
        }
    }

    public T[] get() { return (T[]) this.objects; }

}