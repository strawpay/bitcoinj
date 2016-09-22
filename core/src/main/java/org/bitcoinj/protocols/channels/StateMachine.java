/*
 * Copyright 2013 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bitcoinj.protocols.channels;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.bitcoinj.utils.Threading;

import java.util.Locale;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A class representing a state machine, with limited transitions between states.
 * @param <State> An enum of states to use
 */
public class StateMachine<State extends Enum<State>> {
    private State currentState;

    private final Multimap<State, State> transitions;
    private final ReentrantLock lock = Threading.lock("StateMachine");

    public StateMachine(State startState, Multimap<State, State> transitions) {
        currentState = checkNotNull(startState);
        this.transitions = checkNotNull(transitions);
    }

    /**
     * Checks that the machine is in the given state. Throws if it isn't.
     * @param requiredState
     */
    public void checkState(State requiredState) throws IllegalStateException {
        lock.lock();
        try {
            if (requiredState != currentState) {
                throw new IllegalStateException(String.format(Locale.US,
                        "Expected state %s, but in state %s", requiredState, currentState));
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Checks that the machine is in one of the given states. Throws if it isn't.
     * @param requiredStates
     */
    public void checkState(State... requiredStates) throws IllegalStateException {
        lock.lock();

        try {
            for (State requiredState : requiredStates) {
                if (requiredState.equals(currentState)) {
                    return;
                }
            }
            throw new IllegalStateException(String.format(Locale.US,
                    "Expected states %s, but in state %s", Lists.newArrayList(requiredStates), currentState));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Transitions to a new state, provided that the required transition exists
     * @param newState
     * @throws IllegalStateException If no state transition exists from oldState to newState
     */
    public void transition(State newState) throws IllegalStateException {
        lock.lock();

        try {
            if (transitions.containsEntry(currentState, newState)) {
                currentState = newState;
            } else {
                throw new IllegalStateException(String.format(Locale.US,
                        "Attempted invalid transition from %s to %s", currentState, newState));
            }
        } finally {
            lock.unlock();
        }
    }

    public State getState() {
        lock.lock();
        try {
            return currentState;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        return new StringBuilder().append('[').append(getState()).append(']').toString();
    }
}
