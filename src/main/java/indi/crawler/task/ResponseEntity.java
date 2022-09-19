package indi.crawler.task;

import java.io.File;
import java.io.Serializable;
import java.util.Optional;

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
	private Long size;

	public enum TYPE {
		STRING, 
		BYTE_ARRAY, 
		/**
		 * 文件，将把响应流写到磁盘的临时文件中，可显著减少内存占用；使用该下载类型时必须指定临时文件地址！
		 */
		TMP_FILE
	}

	public ResponseEntity(Object content, TYPE type) {
		this.content = content;
		this.type = Optional.ofNullable(type).orElse(TYPE.STRING);
	}
	
	/**
	 * 获取响应实体的大小；返回值只会是0（未响应）或一个固定的非0值（已响应）
	 * 
	 * @return
	 * @author DragonBoom
	 * @since 2020.09.11
	 */
	public long size() {
	    if (size != null) {
	        return size;
	    }
	    if (content == null) {
	        return 0;
	    }
        synchronized (this) {
            switch (type) {
            case BYTE_ARRAY:
                byte[] bytes = (byte[]) content;
                size = (long) bytes.length;
                break;
            case STRING:
                String str = (String) content;
                size = (long) str.getBytes().length;
                break;
            // 下载到文件中
            case TMP_FILE:
                File file = (File) content;
                size = file.length();
                break;
            default:
                throw new IllegalArgumentException("响应实体的类型异常");
            }
            this.size = size;
        }
	    return size;
	}

}
