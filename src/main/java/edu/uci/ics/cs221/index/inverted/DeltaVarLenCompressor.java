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

            String result = "";
            List<String> VBCodes = new ArrayList<>();
            int key;

            for (int j = difference; j >= 1; j = j / 2){
                if (j % 2 == 0){
                    key = 0;
                }
                else {
                    key = 1;
                }

                result = key + result;

                if (result.length() == 7){
                    if (VBCodes.size() >= 1){
                        result = 1 + result;
                    }
                    else {
                        result = 0 + result;
                    }
                    VBCodes.add(result);
                    result = "";
                }
            }

            if (VBCodes.size() >= 1 && !result.equals("")){
                StringBuilder zeros = new StringBuilder();
                for (int k = 0; k < 7-result.length(); k++){
                    zeros.append("0");
                }
                result = 1 + zeros.toString() + result;
                VBCodes.add(result);
            }
            else if (VBCodes.size() == 0 && !result.equals("")){
                StringBuilder zeros = new StringBuilder();
                for (int l = 0; l < 7-result.length(); l++){
                    zeros.append("0");
                }
                result = 0 + zeros.toString() + result;
                VBCodes.add(result);
            }

            for (int m = VBCodes.size()-1; m >= 0; m--){
                Integer s = Integer.parseInt(VBCodes.get(m), 2);
                byte temp = s.byteValue();
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

        List<String> temp = new ArrayList<>();
        for (int j = 0; j < codes.length; j++){
            String code = String.format("%8s", Integer.toBinaryString(codes[j] & 0xFF)).replace(' ', '0');
            temp.add(code);
        }

        List<String> key = new ArrayList<>();

        for (String str : temp) {
            if (!str.startsWith("0")){
                key.add(str);
                continue;
            }
            key.add(str);

            String result = String.valueOf(Integer.valueOf(key.get(0).substring(1)));

            for (int k = 1; k < key.size(); k++){
                result = result.concat(key.get(k).substring(1));
            }

            results.add(Integer.parseInt(result, 2));
            key.clear();
        }

        for (int l = 1; l < results.size(); l++){
            results.set(l, results.get(l) + results.get(l - 1));
        }

        return results;
    }
}
