package indi.crawler.nest;

import java.util.Optional;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class ResponseEntity {
	private Object content;
	private TYPE type;

	public enum TYPE {
		String, ByteArray
	}

	public ResponseEntity(Object content, TYPE type) {
		this.content = content;
		this.type = Optional.ofNullable(type).orElse(TYPE.String);
	}

}
