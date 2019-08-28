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
		String, 
		ByteArray, 
		/**
		 * 文件，将直接把响应实体存放到磁盘的临时文件中
		 */
		File
	}

	public ResponseEntity(Object content, TYPE type) {
		this.content = content;
		this.type = Optional.ofNullable(type).orElse(TYPE.String);
	}
}