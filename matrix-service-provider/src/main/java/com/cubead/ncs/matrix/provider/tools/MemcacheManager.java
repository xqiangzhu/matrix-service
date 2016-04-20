package com.cubead.ncs.matrix.provider.tools;

import net.rubyeye.xmemcached.MemcachedClient;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component("memcacheManager")
public class MemcacheManager {
	Logger log = Logger.getLogger(this.getClass());
	
	public static final int TIMEOUT = 30 * 24 * 60 * 60;//30天
	@Autowired
	private MemcachedClient memcachedClient;
	
	public void set(String key,String value){
		try {
			memcachedClient.set(key,TIMEOUT,value);
		} catch ( Exception e ) {
			log.error("memcached set error:",e);
		} 
	}
	
	public void set(String key,Object value){
		try {
			memcachedClient.set(key,TIMEOUT,value);
		} catch ( Exception e ) {
			log.error("memcached set error:",e);
		} 
	}
	
	public <T> T get(String key){
		T value = null;
		try {
			value = memcachedClient.get(key);
		} catch ( Exception e ) {
			log.error("memcached get error:",e);
		} 
		return value;
	}
	
	
	public void del(String key) {
		try {
			memcachedClient.delete(key);
		} catch (Exception e) {
			log.error("memcached del error:",e);
		}
		
	}
	
}
