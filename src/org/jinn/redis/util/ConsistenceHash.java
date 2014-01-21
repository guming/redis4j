package org.jinn.redis.util;

import java.util.List;
import java.util.TreeMap;


public class ConsistenceHash {
	 // virtual nodes
    static final int NUM_REPS = 16;
    public HashAlgorithm alg = HashAlgorithm.KETAMA_HASH;
    
    public TreeMap<Long, String> buildMap(final List<String> servers) {
        final TreeMap<Long/* hash */, String/* server */> serversMap = new TreeMap<Long, String>();
        for (final String consumer : servers) {
            if (this.alg == HashAlgorithm.KETAMA_HASH) {
                for (int i = 0; i < NUM_REPS / 4; i++) {
                    final byte[] digest = HashAlgorithm.computeMd5(consumer + "-" + i);
                    for (int h = 0; h < 4; h++) {
                        final long k =
                                (long) (digest[3 + h * 4] & 0xFF) << 24 | (long) (digest[2 + h * 4] & 0xFF) << 16
                                        | (long) (digest[1 + h * 4] & 0xFF) << 8 | digest[h * 4] & 0xFF;
                        serversMap.put(k, consumer);
                    }

                }
            }
            else {
                for (int i = 0; i < NUM_REPS; i++) {
                    final long key = this.alg.hash(consumer + "-" + i);
                    serversMap.put(key, consumer);
                }
            }

        }
        return serversMap;
    }

	public HashAlgorithm getAlg() {
		return alg;
	}
   
}
