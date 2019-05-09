package indi.crawler.nest;

import java.io.Serializable;
import java.util.Optional;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
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

}
