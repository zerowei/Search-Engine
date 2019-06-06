package edu.uci.ics.cs221.index.inverted;

import java.util.ArrayList;
import java.util.List;

/**
 * Implement this compressor with Delta Encoding and Variable-Length Encoding.
 * See Project 3 description for details.
 */
public class DeltaVarLenCompressor implements Compressor {

    @Override
    public byte[] encode(List<Integer> integers) {
        List<Integer> differences = new ArrayList<>();
        List<Byte> finalResult  = new ArrayList<>();

        if (integers.isEmpty()){
            return new byte[0];
        }

        differences.add(integers.get(0));

        for (int i  = 1; i < integers.size(); i++){
            int difference = integers.get(i) - integers.get(i - 1);
            differences.add(difference);
        }

        for (Integer difference : differences){
            if (difference == 0){
                finalResult.add(difference.byteValue());
                continue;
            }

            int result = 0;
            List<Integer> VBCodes = new ArrayList<>();
            int key;
            int t = 0;

            for (int j = difference; j >= 1; j = j / 2){
                if (j % 2 == 0){
                    key = 0;
                }
                else {
                    key = 1;
                }

                result += key * Math.pow(2, t);
                t++;

                if (t == 7){
                    if (VBCodes.size() >= 1){
                        result = 128 | result;
                    }

                    VBCodes.add(result);
                    result = 0;
                    t = 0;
                }
            }

            if (VBCodes.size() >= 1 && result != 0){
                result = 128 | result;
                VBCodes.add(result);
            }
            else if (VBCodes.size() == 0 && result != 0){
                VBCodes.add(result);
            }

            for (int m = VBCodes.size()-1; m >= 0; m--){
                int num = VBCodes.get(m);
                byte temp = (byte) num;
                finalResult.add(temp);
            }
        }

        byte[] codes = new byte[finalResult.size()];
        int n = 0;
        for (Byte code : finalResult){
            codes[n] = code;
            n++;
        }

        return codes;
    }

    @Override
    public List<Integer> decode(byte[] bytes, int start, int length) {
        List<Integer> results = new ArrayList<>();
        byte[] codes = new byte[length];

        for (int i = start; i < start + length; i++){
            codes[i-start] = bytes[i];
        }

        List<List<Byte>> binaryCodes= new ArrayList<>();
        Integer sum = 0;

        for (int j = 0; j < codes.length; j++) {
            List<Byte> binaryCode = new ArrayList<>();

            for (int k = 0; k < 8; k++) {
                binaryCode.add((byte) (codes[j] & 1));
                codes[j] = (byte) (codes[j] >> 1);
            }

            binaryCodes.add(binaryCode);

            if (binaryCode.get(7) == 0){
                for (int m = 0; m < binaryCodes.size(); m++){
                    for (int n = 0; n < 7; n++){
                        if (binaryCodes.get(m).get(n) == 1){
                            sum += (int) Math.pow(2, 7*(binaryCodes.size()-1-m)+n);
                        }
                    }
                }

                results.add(sum);
                sum = 0;
                binaryCodes.clear();
            }
        }

        for (int l = 1; l < results.size(); l++){
            results.set(l, results.get(l) + results.get(l - 1));
        }

        return results;
    }
}