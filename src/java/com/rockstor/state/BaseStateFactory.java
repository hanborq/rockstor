/**
 * Copyright 2012 Hanborq Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rockstor.state;

public class BaseStateFactory {
    private BaseState[] states = null;
    private static BaseStateFactory instance;

    private BaseStateFactory() {
        states = new BaseState[StateEnum.UNDEFINED.ordinal()];

        addState(new HttpReadState());
        addState(new HttpWriteState());

        addState(new MetaReadState());
        addState(new MetaWriteState());

        addState(new ChunkReadState());
        addState(new ChunkWriteState());
        addState(new TimeoutState());
    }

    private void addState(BaseState state) {
        states[state.getStateCode().ordinal()] = state;
    }

    public static BaseStateFactory getInstance() {
        if (instance == null) {
            instance = new BaseStateFactory();
        }
        return instance;
    }

    public BaseState getState(StateEnum stateCode) {
        assert (stateCode != StateEnum.UNDEFINED);
        return states[stateCode.ordinal()];
    }

    public static void main(String[] args) {

    }

}
