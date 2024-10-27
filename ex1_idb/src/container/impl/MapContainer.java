package container.impl;

import container.Container;
import util.MetaData;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class MapContainer<Value> implements Container<Long, Value> {

	private Map<Long, Value> map;
	private long nextKey;

	public MapContainer() {
		// TODO
		map = new HashMap<>();
		nextKey = 0;
	}
	
	@Override
	public MetaData getMetaData() {
		// TODO
		MetaData metaData = new MetaData();
		metaData.setIntProperty("size", map.size());
		return metaData;
	}
	
	@Override
	public void open() {
		// TODO
		System.out.println("Opening MapContainer");
	}

	@Override
	public void close() {
		// TODO
		map.clear();
	}
	
	@Override
	public Long reserve() throws IllegalStateException {
		// TODO
		Long key = nextKey;
		try {
			nextKey++;
		} catch (Exception e) {
			throw new IllegalStateException("Something went wrong!");
		}
		map.put(key, null);
		return key;
	}
	

	@Override
	public Value get(Long key) throws NoSuchElementException {
		// TODO
		if (!map.containsKey(key)) {
			throw new NoSuchElementException("Key not found!");
		}
		return map.get(key);
	}

	@Override
	public void update(Long key, Value value) throws NoSuchElementException {
		// TODO
		if(!map.containsKey(key)) {
			throw new NoSuchElementException("Key not found!");
		}
		map.put(key, value);
	}

	@Override
	public void remove(Long key) throws NoSuchElementException {
		// TODO
		if(!map.containsKey(key)) {
			throw new NoSuchElementException("Key not found!");
		}
		map.remove(key);
	}
}
