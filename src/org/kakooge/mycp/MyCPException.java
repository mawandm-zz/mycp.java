package org.kakooge.mycp;

public class MyCPException extends Exception{
	public MyCPException(String message, Exception e){
		super(message, e);
	}
	public MyCPException(String message){
		super(message);
	}
}
