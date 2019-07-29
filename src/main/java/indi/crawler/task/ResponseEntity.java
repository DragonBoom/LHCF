package indi.crawler.task;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Optional;

import indi.exception.WrapperException;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString(exclude = {"content"})
@NoArgsConstructor
public class ResponseEntity implements Serializable {
    private static final long serialVersionUID = 1L;
    private Object content;
	private TYPE type;

	public enum TYPE {
		String, ByteArray
	}

	public ResponseEntity(Object content, TYPE type) {
		this.content = content;
		this.type = Optional.ofNullable(type).orElse(TYPE.String);
	}
	
	public static void fixContentType(ResponseEntity entity) {
	    Object content = entity.getContent();
	    
	    switch(entity.getType()) {
        case ByteArray:
            if (!(content instanceof byte[])) {
                if (content instanceof String) {
                    try {
                        entity.setContent(((String) content).getBytes("utf-8"));
                    } catch (UnsupportedEncodingException e) {
                        throw new WrapperException(e);
                    }
                }
            }
            break;
        case String:
            if (!(content instanceof String)) {
                if (content instanceof byte[]) {
                    try {
                        entity.setContent(new String((byte[])content, "utf-8"));
                    } catch (UnsupportedEncodingException e) {
                        throw new WrapperException(e);
                    }
                }
            }
            break;
        default:
            break;
	    }
	}

}
