package org.vanilladb.dd.schedule.tpart.sink;

public abstract class SinkerAddon extends Sinker {

	protected Sinker sinker = null;

	public SinkerAddon(Sinker sinker) {
		this.sinker = sinker;
	}

}
